import { WebsocketReverseMappingTransport } from '@chainlink/external-adapter-framework/transports/websocket'
import { makeLogger } from '@chainlink/external-adapter-framework/util'
import { BaseEndpointTypes } from '../endpoint/price'
import { ShardedWebsocketReverseMappingTransport } from './sharded'
import {
  BaseMessage,
  blocksizeDefaultUnsubscribeMessageBuilder,
  blocksizeDefaultWebsocketOpenHandler,
  buildBlocksizeWebsocketTickersMessage,
  handlePriceUpdates,
  VwapUpdate,
} from './utils'

const logger = makeLogger('BlocksizeCapitalTransportPrice')

export interface PriceMessage extends BaseMessage {
  result?: {
    snapshot: string[]
  }
}

export interface VwapMessage extends PriceMessage {
  method: 'vwap'
  params: {
    updates: VwapUpdate[]
  }
}

export type WsTransportTypes = BaseEndpointTypes & {
  Provider: {
    WsMessage: VwapMessage
  }
}

// BlockSize Capital's WebSocket server closes connections after ~60s once they
// hold more than ~85 concurrent vwap subscriptions. Sharding across multiple
// WS connections keeps each one under the cap.
const NUM_SHARDS = Number(process.env.WS_NUM_SHARDS || 4)

const createInnerPriceTransport = (): WebsocketReverseMappingTransport<
  WsTransportTypes,
  string
> => {
  let t: WebsocketReverseMappingTransport<WsTransportTypes, string>
  t = new WebsocketReverseMappingTransport<WsTransportTypes, string>({
    url: (context) => context.adapterSettings.WS_API_ENDPOINT,
    handlers: {
      open: (connection, context) =>
        blocksizeDefaultWebsocketOpenHandler(connection, context.adapterSettings.API_KEY),
      message: (message) => {
        if (message.method !== 'vwap') {
          if (message?.result?.snapshot && JSON.stringify(message.result.snapshot) === '[]') {
            logger.error(`Pair does not exist`)
            logger.warn(`Possible Solutions:
              1. Confirm you are using the same symbol found in the job spec with the correct case.
              2. There maybe an issue with the job spec or the Data Provider may have delisted the asset. Reach out to Chainlink Labs.`)
          }
          return []
        }
        const updates = message.params.updates
        return handlePriceUpdates(updates, t)
      },
    },
    builders: {
      subscribeMessage: (params) => {
        const pair = `${params.base}${params.quote}`.toUpperCase()
        t.setReverseMapping(pair, params)
        return buildBlocksizeWebsocketTickersMessage('vwap_subscribe', pair)
      },
      unsubscribeMessage: (params) =>
        blocksizeDefaultUnsubscribeMessageBuilder(params.base, params.quote, 'vwap_unsubscribe'),
    },
  })
  return t
}

export const transport = new ShardedWebsocketReverseMappingTransport<WsTransportTypes, string>(
  NUM_SHARDS,
  () => createInnerPriceTransport(),
)
