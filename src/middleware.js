/* eslint-env browser */
import { Dagula } from 'dagula'
import { composeMiddleware } from '@web3-storage/gateway-lib/middleware'
import { CarReader } from '@ipld/car'
import { parseCid, HttpError, toIterable } from '@web3-storage/gateway-lib/util'
import { base32 } from 'multiformats/bases/base32'
import * as BatchingFetcher from '@web3-storage/blob-fetcher/fetcher/batching'
import * as ContentClaimsLocator from '@web3-storage/blob-fetcher/locator/content-claims'
import { BatchingR2Blockstore } from './lib/blockstore.js'
import { version } from '../package.json'
import { MultiCarIndex, StreamingCarIndex } from './lib/dag-index/car.js'
import { CachingBucket, asSimpleBucket } from './lib/bucket.js'
import { MAX_CAR_BYTES_IN_MEMORY, CAR_CODE } from './constants.js'
import { handleCarBlock } from './handlers/car-block.js'
import { RateLimitExceeded } from './bindings.js'

/**
 * @typedef {import('./bindings.js').Environment} Environment
 * @typedef {import('@web3-storage/gateway-lib').IpfsUrlContext} IpfsUrlContext
 * @typedef {import('./bindings.js').IndexSourcesContext} IndexSourcesContext
 * @typedef {import('@web3-storage/gateway-lib').BlockContext} BlockContext
 * @typedef {import('@web3-storage/gateway-lib').DagContext} DagContext
 * @typedef {import('@web3-storage/gateway-lib').UnixfsContext} UnixfsContext
 */


/**
 * @type {import('./bindings.js').RateLimits}
 */
const RateLimits = {
  create: ({ serviceURL }) => ({
    check: async (cid, options) => {
      console.log(`checking ${serviceURL} to see if rate limits are exceeded for ${cid} with options`, options)
      return RateLimitExceeded.MAYBE
    }
  })
}

/**
 * @type {import('./bindings.js').Accounting}
 */
const Accounting = {
  create: ({ serviceURL }) => ({
    record: async (cid, options) => {
      console.log(`using ${serviceURL} to record a GET for ${cid} with options`, options)
    }
  })
}

/**
 * 
 * @type {import('@web3-storage/gateway-lib').Middleware<IpfsUrlContext, IpfsUrlContext, Environment>}
 */
export function withRateLimits(handler) {
  return async (request, env, ctx) => {
    const { dataCid } = ctx
    const rateLimits = RateLimits.create({
      serviceURL: env.RATE_LIMITS_SERVICE_URL ? new URL(env.RATE_LIMITS_SERVICE_URL) : undefined
    })
    const isRateLimitExceeded = await rateLimits.check(dataCid, request)
    if (isRateLimitExceeded === RateLimitExceeded.YES) {
      // TODO should we record this?
      throw new HttpError('Too Many Requests', { status: 429 })
    } else {
      const accounting = Accounting.create({
        serviceURL: env.ACCOUNTING_SERVICE_URL ? new URL(env.ACCOUNTING_SERVICE_URL) : undefined
      })
      // ignore the response from the accounting service - this is "fire and forget"
      void accounting.record(dataCid, request)
      return handler(request, env, ctx)
    }
  }
}


/**
 * Validates the request does not contain a HTTP `Range` header.
 * Returns 501 Not Implemented in case it has.
 * @type {import('@web3-storage/gateway-lib').Middleware<import('@web3-storage/gateway-lib').Context>}
 */
export function withHttpRangeUnsupported (handler) {
  return (request, env, ctx) => {
    // Range request https://github.com/web3-storage/gateway-lib/issues/12
    if (request.headers.get('range')) {
      throw new HttpError('Not Implemented', { status: 501 })
    }

    return handler(request, env, ctx)
  }
}

/**
 * Middleware that will serve CAR files if a CAR codec is found in the path
 * CID. If the CID is not a CAR CID it delegates to the next middleware.
 *
 * @type {import('@web3-storage/gateway-lib').Middleware<IpfsUrlContext, IpfsUrlContext, Environment>}
 */
export function withCarBlockHandler (handler) {
  return async (request, env, ctx) => {
    const { dataCid, searchParams } = ctx
    if (!dataCid) throw new Error('missing data CID')
    // if not CAR codec, or if a different format has been requested...
    if (dataCid.code !== CAR_CODE || searchParams.get('format') || request.headers.get('Accept')) {
      return handler(request, env, ctx) // pass to other handlers
    }
    return handleCarBlock(request, env, ctx)
  }
}

/**
 * Creates a dagula instance backed by content claims.
 *
 * @type {import('@web3-storage/gateway-lib').Middleware<BlockContext & DagContext & UnixfsContext & IndexSourcesContext & IpfsUrlContext, IndexSourcesContext & IpfsUrlContext, Environment>}
 */
export function withContentClaimsDagula (handler) {
  return async (request, env, ctx) => {
    const { dataCid } = ctx
    const locator = ContentClaimsLocator.create({
      serviceURL: env.CONTENT_CLAIMS_SERVICE_URL ? new URL(env.CONTENT_CLAIMS_SERVICE_URL) : undefined
    })
    const locRes = await locator.locate(dataCid.multihash)
    if (locRes.error) {
      if (locRes.error.name === 'NotFound') {
        // fallback to old index sources and dagula fallback
        return composeMiddleware(
          withIndexSources,
          withDagulaFallback
        )(handler)(request, env, ctx)
      }
      throw new Error(`failed to locate: ${dataCid}`, { cause: locRes.error })
    }

    const fetcher = BatchingFetcher.create(locator)
    const dagula = new Dagula({
      async get (cid) {
        const res = await fetcher.fetch(cid.multihash)
        return res.ok ? { cid, bytes: await res.ok.bytes() } : undefined
      },
      async stream (cid, options) {
        const res = await fetcher.fetch(cid.multihash, options)
        // In Miniflare, the response data is `structuredClone`'d - this
        // causes the underlying `ArrayBuffer` to become "detached" and
        // all Uint8Array views are reset to zero! So after the first
        // chunk is sent, any additional chunks that are views on the
        // same `ArrayBuffer` become Uint8Array(0), instead of the
        // content they're supposed to contain.
        // @ts-expect-error `MINIFLARE` is not a property of `globalThis`
        if (globalThis.MINIFLARE && res.ok) {
          return res.ok.stream().pipeThrough(new TransformStream({
            transform: (chunk, controller) => controller.enqueue(chunk.slice())
          }))
        }
        return res.ok ? res.ok.stream() : undefined
      },
      async stat (cid) {
        const res = await locator.locate(cid.multihash)
        return res.ok ? { size: res.ok.site[0].range.length } : undefined
      }
    })
    return handler(request, env, { ...ctx, blocks: dagula, dag: dagula, unixfs: dagula })
  }
}

/**
 * Extracts a set of index sources from search params from the URL querystring
 * or DUDEWHERE bucket.
 * @type {import('@web3-storage/gateway-lib').Middleware<IndexSourcesContext & IpfsUrlContext, IpfsUrlContext, Environment>}
 */
export function withIndexSources (handler) {
  return async (request, env, ctx) => {
    if (!ctx.dataCid) throw new Error('missing data CID')
    if (!ctx.searchParams) throw new Error('missing URL search params')

    // Cloudflare currently sets a limit of 3300 sub-requests within the worker
    // context. If we have a given root CID split across hundreds of CARs,
    // freeway will hit the sub-requests limit and not serve the content.
    const maxShards = env.MAX_SHARDS ? parseInt(env.MAX_SHARDS) : 825
    // open a cache explicitly for storing index data
    const cache = await caches.open('index-source')

    /** @type {import('./bindings.js').IndexSource[]} */
    let indexSources = ctx.searchParams
      .getAll('origin')
      .flatMap(str => {
        return str.split(',')
          .reduce((cids, str) => {
            const cid = /** @type {import('cardex/api').CARLink} */(parseCid(str))
            if (cid.code !== CAR_CODE) {
              throw new HttpError(`not a CAR CID: ${cid}`, { status: 400 })
            }
            cids.push(cid)
            return cids
          }, /** @type {import('cardex/api').CARLink[]} */([]))
      })
      .map(cid => ({
        origin: cid,
        bucket: new CachingBucket(asSimpleBucket(env.SATNAV), cache, ctx),
        key: `${cid}/${cid}.car.idx`
      }))

    // if origins were not specified or invalid
    if (!indexSources.length) {
      /** @type {string|undefined} */
      let cursor
      while (true) {
        const results = await env.DUDEWHERE.list({ prefix: `${ctx.dataCid.toV1().toString(base32)}/`, cursor })
        if (!results || !results.objects.length) break

        // if the first encountered item is a index rollup, use it
        if (!cursor && results.objects[0].key.endsWith('rollup.idx')) {
          indexSources.push({
            bucket: new CachingBucket(asSimpleBucket(env.DUDEWHERE), cache, ctx),
            key: results.objects[0].key
          })
          break
        }

        indexSources.push(...results.objects.map(o => {
          const cid = /** @type {import('cardex/api').CARLink} */(parseCid(o.key.split('/')[1]))
          const key = `${cid}/${cid}.car.idx`
          return {
            origin: cid,
            bucket: new CachingBucket(asSimpleBucket(env.SATNAV), cache, ctx),
            key
          }
        }))

        if (indexSources.length > maxShards) {
          console.warn('exceeds maximum DAG shards') // fallback to content claims
          indexSources = []
          break
        }

        if (!results.truncated) break
        cursor = results.cursor
      }
      console.log(`${ctx.dataCid} => ${indexSources.length} index sources`)
    }

    return handler(request, env, { ...ctx, indexSources })
  }
}

/**
 * Creates a dagula instance backed by the R2 blockstore fallback with index sources.
 * @type {import('@web3-storage/gateway-lib').Middleware<BlockContext & DagContext & UnixfsContext & IndexSourcesContext & IpfsUrlContext, IndexSourcesContext & IpfsUrlContext, Environment>}
 */
export function withDagulaFallback (handler) {
  return async (request, env, ctx) => {
    const { indexSources, searchParams } = ctx
    if (!indexSources) throw new Error('missing index sources in context')
    if (!searchParams) throw new Error('missing URL search params in context')

    /** @type {import('dagula').Blockstore?} */
    let blockstore = null
    if (indexSources.length) {
      if (indexSources.length === 1 && indexSources[0].origin != null) {
        const carPath = `${indexSources[0].origin}/${indexSources[0].origin}.car`
        const headObj = await env.CARPARK.head(carPath)
        if (!headObj) throw new HttpError('CAR not found', { status: 404 })
        if (headObj.size < MAX_CAR_BYTES_IN_MEMORY) {
          const obj = await env.CARPARK.get(carPath)
          if (!obj) throw new HttpError('CAR not found', { status: 404 })
          // @ts-expect-error old multiformats in js-car
          blockstore = await CarReader.fromIterable(toIterable(obj.body))
        }
      }
      if (!blockstore) {
        const index = new MultiCarIndex()
        for (const src of indexSources) {
          index.addIndex(new StreamingCarIndex(src))
        }
        blockstore = new BatchingR2Blockstore(env.CARPARK, index)
      }
    } else {
      throw new HttpError('missing index', { status: 404 })
    }

    const dagula = new Dagula(blockstore)
    return handler(request, env, { ...ctx, blocks: dagula, dag: dagula, unixfs: dagula })
  }
}

/**
 * @type {import('@web3-storage/gateway-lib').Middleware<import('@web3-storage/gateway-lib').Context>}
 */
export function withVersionHeader (handler) {
  return async (request, env, ctx) => {
    const response = await handler(request, env, ctx)
    response.headers.set('x-freeway-version', version)
    return response
  }
}
