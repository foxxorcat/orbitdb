import * as Block from 'multiformats/block'
import * as dagCbor from '@ipld/dag-cbor'
import { sha256 } from 'multiformats/hashes/sha2'
import { base58btc } from 'multiformats/bases/base58'
import { ComposedStorage, IPFSBlockStorage, LRUStorage } from './storage/index.js'
import { base32 } from 'multiformats/bases/base32'

const codec = dagCbor
const hasher = sha256
const hashStringEncoding = base58btc

const ManifestStore = async ({ ipfs, storage } = {}) => {
  /**
   * @namespace module:Manifest~Manifest
   * @description The instance returned by {@link module:Manifest~Manifest}.
   * @private
   */

  storage = storage || await ComposedStorage(
    await LRUStorage({ size: 100000 }),
    await IPFSBlockStorage({ ipfs, pin: true })
  )

  const get = async (address) => {
    const bytes = await storage.get(address)
    const { value } = await Block.decode({ bytes, codec, hasher })

    // 兼容 go-v1 版本字段
    if (value.access_controller != null) {
      value.accessController = value.access_controller
      value.ver = 'go-v1'
      delete value.access_controller;
    }

    if (value) {
      // Write to storage to make sure it gets pinned on IPFS
      await storage.put(address, bytes)
    }
    return value
  }

  const create = async ({ name, type, accessController, meta, ver }) => {
    if (!name) throw new Error('name is required')
    if (!type) throw new Error('type is required')
    if (!accessController) throw new Error('accessController is required')

    const manifest = Object.assign(
      {
        name,
        type,
        [ver == 'go-v1' ? 'access_controller' : 'accessController']: accessController
      },
      // meta field is only added to manifest if meta parameter is defined
      meta !== undefined ? { meta } : {}
    )

    const { cid, bytes } = await Block.encode({ value: manifest, codec, hasher })
    const hash = cid.toString(ver == 'go-v1' ? base32 : hashStringEncoding)
    await storage.put(hash, bytes)

    return {
      hash,
      manifest
    }
  }

  const close = async () => {
    await storage.close()
  }

  return {
    get,
    create,
    close
  }
}

export default ManifestStore
