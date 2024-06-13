/* eslint-env worker */
import {
  withContext,
  withCorsHeaders,
  withContentDispositionHeader,
  withErrorHandler,
  createWithHttpMethod,
  withCdnCache,
  withParsedIpfsUrl,
  withFixedLengthStream,
  composeMiddleware
} from '@web3-storage/gateway-lib/middleware'
import {
  handleUnixfs,
  handleBlock,
  handleCar
} from '@web3-storage/gateway-lib/handlers'
import {
  withContentClaimsDagula,
  withHttpRangeUnsupported,
  withVersionHeader,
  withCarBlockHandler,
  withRateLimits
} from './middleware.js'

/**
 * @typedef {import('./bindings.js').Environment} Environment
 * @typedef {import('@web3-storage/gateway-lib').IpfsUrlContext} IpfsUrlContext
 * @typedef {import('@web3-storage/gateway-lib').BlockContext} BlockContext
 * @typedef {import('@web3-storage/gateway-lib').DagContext} DagContext
 * @typedef {import('@web3-storage/gateway-lib').UnixfsContext} UnixfsContext
 */

export default {
  /** @type {import('@web3-storage/gateway-lib').Handler<import('@web3-storage/gateway-lib').Context, import('./bindings.js').Environment>} */
  fetch (request, env, ctx) {
    console.log(request.method, request.url)
    const middleware = composeMiddleware(
      withRateLimits,
      withCdnCache,
      withContext,
      withCorsHeaders,
      withVersionHeader,
      withErrorHandler,
      withParsedIpfsUrl,
      createWithHttpMethod('GET', 'HEAD'),
      withCarBlockHandler,
      withContentClaimsDagula,
      withFormatRawHandler,
      withHttpRangeUnsupported,
      withFormatCarHandler,
      withContentDispositionHeader,
      withFixedLengthStream
    )
    return middleware(handleUnixfs)(request, env, ctx)
  }
}

/**
 * @type {import('@web3-storage/gateway-lib').Middleware<BlockContext & UnixfsContext & IpfsUrlContext, BlockContext & UnixfsContext & IpfsUrlContext, Environment>}
 */
export function withFormatRawHandler (handler) {
  return async (request, env, ctx) => {
    const { headers } = request
    const { searchParams } = ctx
    if (!searchParams) throw new Error('missing URL search params')
    if (searchParams.get('format') === 'raw' || headers.get('Accept')?.includes('application/vnd.ipld.raw')) {
      return await handleBlock(request, env, ctx)
    }
    return handler(request, env, ctx) // pass to other handlers
  }
}

/**
 * @type {import('@web3-storage/gateway-lib').Middleware<DagContext & IpfsUrlContext, DagContext & IpfsUrlContext, Environment>}
 */
export function withFormatCarHandler (handler) {
  return async (request, env, ctx) => {
    const { headers } = request
    const { searchParams } = ctx
    if (!searchParams) throw new Error('missing URL search params')
    if (searchParams.get('format') === 'car' || headers.get('Accept')?.includes('application/vnd.ipld.car')) {
      return await handleCar(request, env, ctx)
    }
    return handler(request, env, ctx) // pass to other handlers
  }
}
