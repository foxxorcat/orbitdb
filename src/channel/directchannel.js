import { EventEmitter } from 'events';
import { pipe } from 'it-pipe';
import { varint } from 'multiformats';

export const PROTOCOL = "/go-orbit-db/direct-channel/1.2.0"

export class DirectChannel extends EventEmitter {
    /**
     * 
     * @param {import('@libp2p/interface').Libp2p} libp2p 
     */
    constructor(libp2p) {
        super()
        this.libp2p = libp2p
        this.libp2p.handle(PROTOCOL, this.handle.bind(this))
    }

    /**
     * 
     * @param {import('@libp2p/interface').IncomingStreamData} param0 
     */
    async handle({ connection, stream }) {
        await pipe(stream, async (source) => {
            const { value: length } = await source.next()
            const [len] = varint.decode(length.subarray())
            const { value: data } = await source.next()
            await source.return()

            if (len != data?.byteLength) {
                return
            }

            this.emit('channel-message', { remotePeer: connection.remotePeer, bytes: data.subarray() })
        })
        stream.close()
    }

    /**
     * 
     * @param {import('@libp2p/interface').PeerId} peerId 
     * @param {Uint8Array} bytes 
     */
    async send(peerId, bytes, { signal }) {
        const stream = await this.libp2p.dialProtocol(peerId, PROTOCOL, { signal })
        await pipe(async function* () {
            const buf = new Uint8Array(varint.encodingLength(bytes.byteLength))
            yield varint.encodeTo(bytes.byteLength, buf)
            yield bytes
        }(), stream)
        stream.close()
    }

    async close() { await this.libp2p.unhandle(PROTOCOL); this.removeAllListeners(); }
}