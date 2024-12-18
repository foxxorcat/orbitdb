import { pipe } from 'it-pipe'
import PQueue from 'p-queue'
import { EventEmitter } from 'events'
import { TimeoutController } from 'timeout-abort-controller'
import { fromString as uint8arraysFromString } from 'uint8arrays/from-string';
import { toString as uint8arraysToString } from 'uint8arrays/to-string';
import { goJSONReplacer, goJSONReviver, toUint8Array, u8ToString } from './utils/go-json.js';
import Entry from './oplog/entry.js';

import { varint } from 'multiformats';
import { CID } from 'multiformats/cid';
import { base32 } from 'multiformats/bases/base32';
import { sha256 } from 'multiformats/hashes/sha2';
import * as Block from 'multiformats/block'
import * as dagCbor from '@ipld/dag-cbor'


const DefaultTimeout = 30000 // 30 seconds
const PROTOCOL = "/go-orbit-db/direct-channel/1.2.0"

/**
 * @module Sync
 * @description
 * The Sync Protocol for OrbitDB synchronizes the database operations {@link module:Log} between multiple peers.
 *
 * The Sync Protocol sends and receives heads between multiple peers,
 * both when opening a database and when a database is updated, ie.
 * new entries are appended to the log.
 *
 * When Sync is started, a peer subscribes to a pubsub topic of the log's id.
 * Upon subscribing to the topic, peers already connected to the topic receive
 * the subscription message and "dial" the subscribing peer using a libp2p
 * custom protocol. Once connected to the subscribing peer on a direct
 * peer-to-peer connection, the dialing peer and the subscribing peer exchange
 * the heads of the Log each peer currently has. Once completed, the peers have
 * the same "local state".
 *
 * Once the initial sync has completed, peers notify one another of updates to
 * the log, ie. updates to the database, using the initially opened pubsub
 * topic subscription. A peer with new heads broadcasts changes to other peers
 * by publishing the updated heads to the pubsub topic. Peers subscribed to the
 * same topic will then receive the update and will update their log's state,
 * the heads, accordingly.
 *
 * The Sync Protocol is eventually consistent. It guarantees that once all
 * messages have been sent and received, peers will observe the same log state
 * and values. The Sync Protocol does not guarantee the order in which messages
 * are received or even that a message is recieved at all, nor any timing on
 * when messages are received.
 *
 * @example
 * // Using defaults
 * const sync = await Sync({ ipfs, log, onSynced: (peerId, heads) => ... })
 *
 * @example
 * // Using all parameters
 * const sync = await Sync({ ipfs, log, events, onSynced: (peerId, heads) => ..., start: false })
 * sync.events.on('join', (peerId, heads) => ...)
 * sync.events.on('leave', (peerId) => ...)
 * sync.events.on('error', (err) => ...)
 * await sync.start()
 */

/**
 * Creates a Sync instance for sychronizing logs between multiple peers.
 *
 * @function
 * @param {Object} params One or more parameters for configuring Sync.
 * @param {IPFS} params.ipfs An IPFS instance.
 * @param {Log} params.log The log instance to sync.
 * @param {EventEmitter} [params.events] An event emitter to use. Events
 * emitted are 'join', 'leave' and 'error'. If the parameter is not provided,
 * an EventEmitter will be created.
 * @param {onSynced} [params.onSynced] A callback function that is called after
 * the peer has received heads from another peer.
 * @param {Boolean} [params.start] True if sync should start automatically,
 * false otherwise. Defaults to true.
 * @return {module:Sync~Sync} sync An instance of the Sync Protocol.
 * @memberof module:Sync
 * @instance
 */
const SyncGoV1 = async ({ ipfs, log, events, onSynced, start, timeout, marshaler }) => {
  /**
   * @namespace module:Sync~Sync
   * @description The instance returned by {@link module:Sync}.
   */

  /**
   * Callback function when new heads have been received from other peers.
   * @callback module:Sync~Sync#onSynced
   * @param {PeerID} peerId PeerID of the peer who we received heads from
   * @param {Entry[]} heads An array of Log entries
   */

  /**
   * Event fired when when a peer has connected and the exchange of
   * heads has been completed.
   * @event module:Sync~Sync#join
   * @param {PeerID} peerId PeerID of the peer who we received heads from
   * @param {Entry[]} heads An array of Log entries
   * @example
   * sync.events.on('join', (peerID, heads) => ...)
   */

  /**
   * Event fired when a peer leaves the sync protocol.
   * @event module:Sync~Sync#leave
   * @param {PeerID} peerId PeerID of the peer who left
   * @example
   * sync.events.on('leave', (peerID) => ...)
   */

  /**
   * Event fired when an error occurs.
   * @event module:Sync~Sync#error
   * @param {Error} error The error that occured
   * @example
   * sync.events.on('error', (error) => ...)
   */

  if (!ipfs) throw new Error('An instance of ipfs is required.')
  if (!log) throw new Error('An instance of log is required.')

  const libp2p = ipfs.libp2p
  const pubsub = ipfs.libp2p.services.pubsub

  const address = log.id
  // const headsSyncAddress = pathJoin('/orbitdb/heads/', address)

  const queue = new PQueue({ concurrency: 1 })

  /**
   * Set of currently connected peers for the log for this Sync instance.
   * @name peers
   * @†ype Set
   * @memberof module:Sync~Sync
   * @instance
   */
  const peers = new Set()

  /**
   * Event emitter that emits Sync changes. See Events section for details.
   * @†ype EventEmitter
   * @memberof module:Sync~Sync
   * @instance
   */
  events = events || new EventEmitter()

  timeout = timeout || DefaultTimeout

  let started = false

  const onPeerJoined = async (peerId) => {
    const heads = await log.heads()
    events.emit('join', peerId, heads)
  }

  /**
   * @param { EntryType | undefined} entry
   * @returns {Promise<Uint8Array>} msgBytes
   */
  const builderMsg = async (entry) => {
    // Entry->EntryGoV1->EntryGoV1JSON
    const heads = (entry ? [entry] : await log.heads())
      ?.map(head => head?.entryGoV1)
      .filter(Boolean)
      .map(toEntryGoV1JSONByEntryGoV1) || []
    const msg = {
      address,
      heads,
    }
    console.debug('builderMsg', msg);
    return await marshaler.marshal(msg)
  }

  /**
   * 
   * @param {Uint8Array} value 
   * @returns {Promise<Uint8Array[]>} entrysBytes
   */
  const parseMsg = async (value) => {
    let { address, heads = [] } = await marshaler.unmarshal(value)
    if (address != address) { return [] }

    const headsBytes = []
    for (const head of heads) {
      // EntryGoV1JSON -> EntryGoV1 -> bytes
      const entryGoV1 = fromEntryGoV1JSONToEntryGoV1(head)
      const entryHash = CID.parse(entryGoV1.hash);
      entryGoV1.hash = null; delete entryGoV1.bytes;
      const { cid, bytes } = await Block.encode({ value: entryGoV1, codec: dagCbor, hasher: sha256 })
      if (!entryHash.equals(cid)) {
        events.emit('error', new Error('sync entry hash error'))
        continue
      }
      headsBytes.push(bytes)
    }
    console.debug('parseMsg', heads, headsBytes);
    return headsBytes
  }

  const sendHeads = (source) => {
    return (async function* () {
      const data = await builderMsg()

      yield varint.encodeTo(data.byteLength, new Uint8Array(varint.encodingLength(data.byteLength)))
      yield data
    })()
  }

  const receiveHeads = (peerId) => async (source) => {
    try {
      const { value: length } = (await source.next())
      const [len] = varint.decode(length.subarray())
      const { value: data } = (await source.next())

      if (onSynced) {
        for (const headBytes of await parseMsg(data.subarray())) {
          await onSynced(headBytes)
        }
      }
      if (started) {
        await onPeerJoined(peerId)
      }
    } finally { source.return() }
  }

  const handleReceiveHeads = async ({ connection, stream }) => {
    const peerId = String(connection.remotePeer)
    try {
      peers.add(peerId)
      await pipe(stream, receiveHeads(peerId))
    } catch (e) {
      peers.delete(peerId)
      events.emit('error', e)
    }
  }

  const handlePeerSubscribed = async (event) => {
    console.log('pubsub ', event);

    const task = async () => {
      const { peerId: remotePeer, subscriptions } = event.detail
      const peerId = String(remotePeer)
      const subscription = subscriptions.find(e => e.topic === address)
      if (!subscription) {
        return
      }
      if (subscription.subscribe) {
        if (peers.has(peerId)) {
          return
        }
        const timeoutController = new TimeoutController(timeout)
        const { signal } = timeoutController
        try {
          peers.add(peerId)
          const stream = await libp2p.dialProtocol(remotePeer, PROTOCOL, { signal })
          await pipe(sendHeads, stream)
        } catch (e) {
          console.error(e)
          peers.delete(peerId)
          if (e.code === 'ERR_UNSUPPORTED_PROTOCOL') {
            // Skip peer, they don't have this database currently
          } else {
            events.emit('error', e)
          }
        } finally {
          if (timeoutController) {
            timeoutController.clear()
          }
        }
      } else {
        peers.delete(peerId)
        events.emit('leave', peerId)
      }
    }
    queue.add(task)
  }

  const handleUpdateMessage = async message => {
    const { topic, data } = message.detail

    const entrysBytes = await parseMsg(data)

    const task = async () => {
      try {
        if (onSynced) {
          for (const entryBytes of entrysBytes) {
            await onSynced(entryBytes)
          }
        }

      } catch (e) {
        events.emit('error', e)
      }
    }

    if (topic === address) {
      queue.add(task)
    }
  }

  /**
   * Add a log entry to the Sync Protocol to be sent to peers.
   * @function add
   * @param {Entry} entry Log entry
   * @memberof module:Sync~Sync
   * @instance
   */
  const add = async (entry) => {
    if (started) {
      await pubsub.publish(address, await builderMsg(entry))
    }
  }

  /**
   * Stop the Sync Protocol.
   * @function stop
   * @memberof module:Sync~Sync
   * @instance
   */
  const stopSync = async () => {
    if (started) {
      started = false
      await queue.onIdle()
      pubsub.removeEventListener('subscription-change', handlePeerSubscribed)
      pubsub.removeEventListener('message', handleUpdateMessage)
      await libp2p.unhandle(PROTOCOL)
      await pubsub.unsubscribe(address)
      peers.clear()
    }
  }

  /**
   * Start the Sync Protocol.
   * @function start
   * @memberof module:Sync~Sync
   * @instance
   */
  const startSync = async () => {
    if (!started) {
      // Exchange head entries with peers when connected
      await libp2p.handle(PROTOCOL, handleReceiveHeads)
      pubsub.addEventListener('subscription-change', handlePeerSubscribed)
      pubsub.addEventListener('message', handleUpdateMessage)
      // Subscribe to the pubsub channel for this database through which updates are sent
      await pubsub.subscribe(address)
      started = true
    }
  }

  // Start Sync automatically
  if (start !== false) {
    await startSync()
  }

  return {
    add,
    stop: stopSync,
    start: startSync,
    events,
    peers
  }
}

export { SyncGoV1 as default }

/**
 * @typedef {import('./oplog/entry.js').EntryGoV1} EntryGoV1
 * @typedef {import('./oplog/entry.js').module}  EntryType
 * @typedef {{
 *  id: string,
 *  payload: Uint8Array,
 *  identity: {
 *    id: string,
 *    publicKey: Uint8Array, // 使用未压缩的secp256k1公钥
 *    signatures: {
 *      id: Uint8Array,
 *      publicKey: Uint8Array,
 *    },
 *    type: string
 *  },
 *  clock: {
 *    id: Uint8Array,
 *    time: number,
 *  },
 *  next: CID[],
 *  refs: CID[],
 *  v: 2,
 *  key: Uint8Array,
 *  sig: Uint8Array,
 *  hash: CID,
 * }} EntryGoV1JSON
 */
/**
 * 
 * @param {EntryGoV1} entryGoV1
 * @return { EntryGoV1JSON } 
 */
const toEntryGoV1JSONByEntryGoV1 = (entryGoV1) => {
  const { id, payload, identity, next, refs, clock, v, key, sig, hash } = entryGoV1
  const { signatures } = identity
  const value = {
    id: id,
    payload: toUint8Array(payload),
    identity: {
      id: identity.id,
      publicKey: toUint8Array(identity.publicKey, 'base16'),
      signatures: {
        id: toUint8Array(signatures.id, 'base16'),
        publicKey: toUint8Array(signatures.publicKey, 'base16')
      },
      type: identity.type,
    },
    clock: {
      id: toUint8Array(clock.id, 'base16'),
      time: clock.time,
    },
    next, refs, v,
    key: toUint8Array(key, 'base16'),
    sig: toUint8Array(sig, 'base16'),
    hash: CID.parse(hash)
  }
  return value
}

/**
 * 
 * @param {EntryGoV1JSON} entry 
 * @returns { EntryGoV1 }
 */
const fromEntryGoV1JSONToEntryGoV1 = (entry) => {
  /** @type {EntryGoV1JSON} */
  const { id, payload, identity, next = [], refs = [], clock, v, key, sig, hash } = entry
  const { signatures } = identity
  return {
    id: id,
    payload: u8ToString(payload),
    identity: {
      id: identity.id,
      publicKey: u8ToString(identity.publicKey, 'base16'),
      signatures: {
        id: u8ToString(signatures.id, 'base16'),
        publicKey: u8ToString(signatures.publicKey, 'base16')
      },
      type: identity.type,
    },
    clock: {
      id: u8ToString(clock.id, 'base16'),
      time: clock.time,
    },
    next, refs, v,
    key: u8ToString(key, 'base16'),
    sig: u8ToString(sig, 'base16'),
    hash: hash.toString(base32)
  }
}