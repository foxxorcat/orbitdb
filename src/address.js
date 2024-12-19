/**
 * @module Address
 * @description OrbitDB database address verification.
 */
import { CID } from 'multiformats/cid'
import { posixJoin } from './utils/path-join.js'

/**
 * Validates an OrbitDB database address.
 * @function
 * @param {module:Address~OrbitDBAddress|string} address An OrbitDB database address.
 * @return {boolean} True if the address is a valid OrbitDB database address,
 * false otherwise.
 * @static
 */
const isValidAddress = (address) => {
  address = address.toString()

  if (!address.startsWith('/orbitdb')) {
    return false
  }

  address = address.replaceAll('/orbitdb/', '')
  const [hash] = address.split('/')

  let cid
  try {
    cid = CID.parse(hash)
  } catch (e) {
    return false
  }

  return cid !== undefined
}

/**
 * Parses an OrbitDB database address.
 * @function
 * @param {module:Address~OrbitDBAddress|string} address A valid OrbitDB database address.
 * @return {module:Address~OrbitDBAddress} An instance of OrbitDBAddress.
 * @throws Not a valid OrbitDB address if no address if provided.
 * @throws Not a valid OrbitDB address if address is invalid.
 * @static
 */
const parseAddress = (address) => {
  if (!address) {
    throw new Error(`Not a valid OrbitDB address: ${address}`)
  }

  if (!isValidAddress(address)) {
    throw new Error(`Not a valid OrbitDB address: ${address}`)
  }

  return OrbitDBAddress(address)
}

/**
 * @typedef {Object} OrbitDBAddress
 * @property {string} protocol Protocol prefix "/orbitdb/".
 * @property {string} hash The hash of the database manifest.
 * @property {string} address The full database address.
 */
const OrbitDBAddress = (address, name, joinName = false) => {
  if (address && address.protocol === 'orbitdb' && address.hash) {
    return address
  }

  const protocol = 'orbitdb'

  const [hash, ...names] = address.replace('/orbitdb/', '').split('/')
  name = name || names.join('/')

  /**
   * Returns address as a string.
   * @typedef {Function} toString
   * @returns {string} Address as a string.
   */
  const toString = () => {
    if (joinName && name != null)
      return posixJoin('/', protocol, hash, name)
    return posixJoin('/', protocol, hash)
  }

  return {
    protocol,
    hash,
    name,
    address,
    toString
  }
}

export { OrbitDBAddress as default, isValidAddress, parseAddress }
