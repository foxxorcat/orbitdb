import { pushable } from 'it-pushable';
import pDefer from 'p-defer';

/**
 * 通过pubsub创建传输通道
 * 
 * @NOTE 与go连通性堪忧, 弃用
 */
export const PROTOCOL = 'ipfs-pubsub-direct-channel/v1'

/**
 * @typedef {import('@libp2p/interface').PubSub } PubSub
 * @typedef {import('@libp2p/interface').Message } Message
 * @typedef {import('@libp2p/interface').Libp2p<{ pubsub: PubSub }> } PubsubLibp2p
 * 
 */

/**
 * @template {Uint8Array} T
 * @template {import('it-stream-types').Duplex<AsyncGenerator<T>,Source<T>,Promise<void>>} Y
 * @typedef {{
 *  libp2p: PubsubLibp2p
 *  pubsub: PubSub
 *  source: import('it-pushable').Pushable<T>
 *  isClose(): boolean
 *  contact(): Promise<void>
 *  close(): Promise<void>
 *  static open(libp2p:PubsubLibp2p, receiverID:string):Promise<PubSubDirectChannel>
 * } & Y} PubSubDirectChannel
 */

/**
 * 等待节点连接
 * @param { PubSub } pubsub 
 * @param { string } channelId
 * @param { string } waitPeerId 等待连接的peerId
 */
const waitForPeer = async (pubsub, channelId, waitPeerId) => {
    const peerId = pubsub.getSubscribers(channelId).find(peerId => peerId.toString() == waitPeerId)
    if (peerId != null) {
        return peerId
    }

    const defer = pDefer()
    pubsub.addEventListener('subscription-change', function handle({ detail: { peerId } }) {
        if (peerId.toString() == waitPeerId) {
            defer.resolve()
            pubsub.removeEventListener('subscription-change', handle)
        }
    })
    return await defer.promise
}

export class PubSubDirectChannel {
    /**
     * 节点ID（收）
     * @private
     * @type {string}
     */
    _receiverID
    /**
     * 节点ID（收发）
     * @private
     * @type {[sender: string, receiver: string]}
     */
    _peers
    /**
     * 通道ID
     * @private
     * @type {string}
     */
    _id
    _closed = false

    get peers() { return this._peers }
    get id() { return this._id }

    /**
     * 
     * @param {PubsubLibp2p} libp2p 
     * @param {string} receiverID 
     */
    constructor(libp2p, receiverID) {
        this.libp2p = libp2p
        this.pubsub = libp2p.services.pubsub
        if (!this.pubsub) {
            throw new Error('This IPFS node does not support pubsub.')
        }

        this._receiverID = receiverID.toString()

        if (!this._receiverID) {
            throw new Error('Receiver ID was undefined')
        }

        this._peers = [this.libp2p.peerId.toString(), this._receiverID]
        this._id = '/' + PROTOCOL + '/' + this._peers.sort().reverse().join('/')

        this.sink = async (source) => {
            for await (const data of source) {
                await this.pubsub.publish(this._id, data)
            }
        }
    }

    /**
     * 
     * @param { CustomEvent<Message> }  
     */
    handleEvent({ detail: { data, topic } }) {
        if (topic == this.id) {
            this.source.push(data)
        }
    }

    isClose() { return this._closed }

    async contact() {
        // this.pubsub.topicValidators.set(this.id, (peerId) => {
        //     if (peerId.toString() != this._receiverID) return TopicValidatorResult.Reject
        //     return TopicValidatorResult.Accept
        // })
        this.pubsub.addEventListener('message', this)
        this.pubsub.subscribe(this.id)

        this.source = pushable({ onEnd: () => { this.source = null; this.close() } })

        await waitForPeer(this.pubsub, this.id, this._receiverID)
    }

    async close() {
        this._closed = true
        this.pubsub.unsubscribe(this.id)
        this.pubsub.removeEventListener('message', this)
        // this.pubsub.topicValidators.delete(this.id)
        this.source?.end()
        this.source = null;
    }

    /**
     * 
     * @param {PubsubLibp2p} libp2p 
     * @param {string} receiverID 
     * @return {PubSubDirectChannel}
     */
    static async open(libp2p, receiverID) {
        const channel = new PubSubDirectChannel(libp2p, receiverID)
        await channel.contact()
        return channel
    }
}