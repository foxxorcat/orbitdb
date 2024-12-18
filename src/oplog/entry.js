import Clock from './clock.js'
import * as Block from 'multiformats/block'
import * as dagCbor from '@ipld/dag-cbor'
import { sha256 } from 'multiformats/hashes/sha2'
import { base58btc } from 'multiformats/bases/base58'
import { base32 } from 'multiformats/bases/base32';
import { base64pad } from 'multiformats/bases/base64';
import { CID } from 'multiformats/cid';
import { keyJSONSort, toUint8Array } from '../utils/go-json.js';
import Identity from '../identities/identity.js';

const codec = dagCbor
const hasher = sha256
const hashStringEncoding = base58btc

/**
 * @typedef {{
 *  hash: null,
 *  id: string,
 *  payload: string | any,
 *  next: CID[],
 *  refs: CID[],
 *  clock: {
 *    id: string,
 *    time: number 
 *  },
 *  v: 2,
 *  sig: string,
 *  key: string,
*   identity: {
*    id: string,
*    publicKey: string,
*    signatures: {
*      id: string,
*      publicKey: string,
*    },
*    type: string
*   },
 *  hash?: string, // base32 编码
 *  bytes?: Uint8Array,
 * }} EntryGoV1
 */

/**
 * @typedef {Object} module:Log~Entry
 * @property {string} id A string linking multiple entries together.
 * @property {*} payload An arbitrary chunk of data.
 * @property {Array<string>} next One or more hashes pointing to the next entries in a chain of
 * entries.
 * @property {Array<string>} refs One or more hashes which reference other entries in the chain.
 * @property {Clock} clock A logical clock. See {@link module:Log~Clock}.
 * @property {integer} v The version of the entry.
 * @property {string} key The public key of the identity.
 * @property {string} identity The identity of the entry's owner.
 * @property {string} sig The signature of the entry signed by the owner.
 */

/**
 * Creates an Entry.
 * @param {module:Identities~Identity} identity The identity instance
 * @param {string} logId The unique identifier for this log
 * @param {*} data Data of the entry to be added. Can be any JSON.stringifyable
 * data.
 * @param {module:Log~Clock} [clock] The clock
 * @param {Array<string|Entry>} [next=[]] An array of CIDs as base58btc encoded
 * strings which point to the next entries in a chain of entries.
 * @param {Array<string|module:Log~Entry>} [refs=[]] An array of CIDs as
 * base58btc encoded strings pointing to various entries which come before
 * this entry.
 * @return {Promise<module:Log~Entry>} A promise which contains an instance of
 * Entry.
 * Entry consists of the following properties:
 *
 * - id: A string linking multiple entries together,
 * - payload: An arbitrary chunk of data,
 * - next: One or more hashes pointing to the next entries in a chain of
 * entries,
 * - refs: One or more hashes which reference other entries in the chain,
 * - clock: A logical clock. See {@link module:Log~Clock},
 * - v: The version of the entry,
 * - key: The public key of the identity,
 * - identity: The identity of the entry's owner,
 * - sig: The signature of the entry signed by the owner.
 * @memberof module:Log~Entry
 * @example
 * const entry = await Entry.create(identity, 'log1', 'hello')
 * console.log(entry)
 * // { payload: "hello", next: [], ... }
 * @private
 */
const create = async (identity, id, payload, clock = null, next = [], refs = [], ver = 'js-v2') => {
  if (identity == null) throw new Error('Identity is required, cannot create entry')
  if (id == null) throw new Error('Entry requires an id')
  if (payload == null) throw new Error('Entry requires a payload')
  if (next == null || !Array.isArray(next)) throw new Error("'next' argument is not an array")

  clock = clock || Clock(identity.publicKey)

  const entry = {
    id, // For determining a unique chain
    payload, // Can be any dag-cbor encodeable data
    next, // Array of strings of CIDs
    refs, // Array of strings of CIDs
    clock, // Clock
    v: 2 // To tag the version of this data structure
  }

  // 使用go-v1的方式编码签名entry数据
  if (ver == 'go-v1') {
    const entryGoV1 = await encodeGoV1(entry, identity)
    entry.key = identity.publicKey
    entry.identity = identity.hash
    entry.entryGoV1 = entryGoV1
    entry.sig = entryGoV1.sig
    entry.bytes = entryGoV1.bytes
    entry.hash = CID.parse(entryGoV1.hash).toString(hashStringEncoding)
    return entry
  }

  const { bytes } = await Block.encode({ value: entry, codec, hasher })
  const signature = await identity.sign(identity, bytes)

  entry.key = identity.publicKey
  entry.identity = identity.hash
  entry.sig = signature

  return encode(entry)
}

/**
 * Verifies an entry signature.
 * @param {Identities} identities Identities system to use
 * @param {module:Log~Entry} entry The entry being verified
 * @return {Promise<boolean>} A promise that resolves to a boolean value indicating if
 * the signature is valid.
 * @memberof module:Log~Entry
 * @private
 */
const verify = async (identities, entry) => {
  if (!identities) throw new Error('Identities is required, cannot verify entry')
  if (!isEntry(entry)) throw new Error('Invalid Log entry')
  if (!entry.key) throw new Error("Entry doesn't have a key")
  if (!entry.sig) throw new Error("Entry doesn't have a signature")

  // 使用 gov1
  if (entry.entryGoV1 != null) return verifyGoV1(identities, entry.entryGoV1)

  const value = {
    id: entry.id,
    payload: entry.payload,
    next: entry.next,
    refs: entry.refs,
    clock: entry.clock,
    v: entry.v
  }

  const { bytes } = await Block.encode({ value, codec, hasher })

  return identities.verify(entry.sig, entry.key, bytes)
}

/**
 * Checks if an object is an Entry.
 * @param {module:Log~Entry} obj
 * @return {boolean}
 * @memberof module:Log~Entry
 * @private
 */
const isEntry = (obj) => {
  return obj && obj.id !== undefined &&
    obj.next !== undefined &&
    obj.payload !== undefined &&
    obj.v !== undefined &&
    obj.clock !== undefined &&
    obj.refs !== undefined
}

/**
 * Determines whether two entries are equal.
 * @param {module:Log~Entry} a An entry to compare.
 * @param {module:Log~Entry} b An entry to compare.
 * @return {boolean} True if a and b are equal, false otherwise.
 * @memberof module:Log~Entry
 * @private
 */
const isEqual = (a, b) => {
  return a && b && a.hash === b.hash
}

/**
 * Decodes a serialized Entry from bytes
 * @param {Uint8Array} bytes
 * @return {module:Log~Entry}
 * @memberof module:Log~Entry
 * @private
 */
const decode = async (bytes) => {
  let { cid, value } = await Block.decode({ bytes, codec, hasher })
  if (typeof value.identity != 'string') {
    value = await decodeGoV1({ ...value, bytes })
    value.entryGoV1.hash = cid.toString(base32)
  }

  const hash = cid.toString(hashStringEncoding)
  return {
    ...value,
    hash,
    bytes
  }
}

/**
 * Encodes an Entry and adds bytes field to it
 * @param {Entry} entry
 * @return {module:Log~Entry}
 * @memberof module:Log~Entry
 * @private
 */
const encode = async (entry) => {
  const { cid, bytes } = await Block.encode({ value: entry, codec, hasher })
  const hash = cid.toString(hashStringEncoding)
  const clock = Clock(entry.clock.id, entry.clock.time)
  return {
    ...entry,
    clock,
    hash,
    bytes
  }
}

/**
 * 
 * @param {*} identities 
 * @param {EntryGoV1} entry 
 * @returns 
 */
const verifyGoV1 = async (identities, entry) => {
  const value = {
    hash: null,
    id: entry.id,
    payload: entry.payload,
    next: entry.next.map(n => n.toString(hashStringEncoding)),
    refs: entry.refs.map(n => n.toString(hashStringEncoding)),
    clock: entry.clock,
    v: entry.v,

    additional_data: entry.additional_data,
  }

  const bytes = toUint8Array(JSON.stringify(keyJSONSort(value)))

  return identities.verify(entry.sig, entry.key, bytes)
}

/**
 * 以go v1版本编码 entry
 * @param {Entry} entry 
 * @param {boolean} includePayload 处理负载保证与go basestore 兼容
 * @returns {Promise<EntryGoV1>}
 */
const encodeGoV1 = async (entry, identity, includePayload = true) => {
  let payload = entry.payload
  if (includePayload) {
    if (typeof payload == 'object' && typeof payload?.op == 'string') {
      if (payload.value instanceof Uint8Array) {
        payload.value = base64pad.baseEncode(payload.value)
      }
      payload = JSON.stringify(payload)
    }
  }

  const { id, clock, v, next, refs } = entry
  const v1_next = next.map(n => CID.parse(n))
  const v1_refs = refs.map(n => CID.parse(n))
  const value = {
    hash: null,
    id: id,
    payload: payload,
    next: v1_next.map(n => n.toString(hashStringEncoding)),
    refs: v1_refs.map(n => n.toString(hashStringEncoding)),
    clock: clock,
    v: v,
  }
  const valueBytes = toUint8Array(JSON.stringify(keyJSONSort(value)))
  const signature = await identity.sign(identity, valueBytes)

  value.next = v1_next
  value.refs = v1_refs
  value.sig = signature
  value.key = identity.publicKey
  {
    const { id, publicKey, signatures, type } = await Identity(identity)
    value.identity = { id, publicKey, signatures, type }
  }

  const { cid, bytes } = await Block.encode({ value, codec, hasher })
  const hash = cid.toString(base32)
  return {
    ...value,
    identity,
    hash,
    bytes
  }
}

/**
 * 
 * @param {EntryGoV1} entry 
 * @return {Entry}
 */
const decodeGoV1 = async (entryGoV1, includePayload = true) => {
  if (typeof entryGoV1.identity == 'string') throw new Error('not go v1 entry')
  entryGoV1.identity = await Identity(entryGoV1.identity)

  const next = entryGoV1.next?.map(n => n.toString(hashStringEncoding)) || []
  const refs = entryGoV1.refs?.map(n => n.toString(hashStringEncoding)) || []
  const clock = Clock(entryGoV1.clock.id, entryGoV1.clock.time)

  let { id, payload, identity, key, sig } = entryGoV1

  if (includePayload) {
    try {
      payload = JSON.parse(payload)
      if (payload != null && typeof payload.op == 'string') {
        if (typeof payload.value == "string") {
          payload.value = base64pad.baseDecode(payload.value)
        }
      }
    } catch (error) { console.error(error) }
  }

  const entry = {
    id,
    payload,
    next,
    refs,
    clock,
    v: 2,
    identity: identity.hash,

    // 依旧使用go v1的数据
    key,
    sig,
    entryGoV1,
  }

  return entry
}

export default {
  create,
  verify,
  decode,
  encode,
  isEntry,
  isEqual,
  decodeGoV1
}
