import { base64pad } from "multiformats/bases/base64"
import { CID } from 'multiformats/cid';

// go 生成JSON的有顺序要求
export const keyJSONSort = (obj) => (typeof obj == 'object' && obj != null) ? new Proxy(obj, {
    get(target, p, receiver) {
        const value = Reflect.get(target, p, receiver)
        if (Object.prototype.toString.call(value) === '[object Object]') {
            return keyJSONSort(value)
        }
        return value
    },
    ownKeys(target) { return Reflect.ownKeys(target).sort() }
}) : obj

/**
 * 
 * @param {string} key 
 * @param {any} value 
 */
export const goJSONReplacer = (key, value) => {
    if (value instanceof Uint8Array) {
        return base64pad.baseEncode(value)
    }
    return value
}
export const goJSONReviver = (key, value) => {
    if (typeof value == 'string') {
        if (value.startsWith('/')) {
            return value
        }
        if (key == '/') {
            try { return CID.parse(value) }
            catch (error) { }
        }
        try { return base64pad.baseDecode(value) }
        catch (error) { }
    }

    if (value?.['/'] instanceof CID) {
       return value['/']
    }
    return value
}

import { fromString as uint8arraysFromString } from 'uint8arrays/from-string';
import { toString as uint8arraysToString } from 'uint8arrays/to-string';
/**
 * 
 * @param {Uint8Array|string} data 
 * @param {SupportedEncodings|undefined} encoding
 * @returns {Uint8Array}
 */
export const toUint8Array = (data, encoding) => {
    if (typeof data == 'string') {
        return uint8arraysFromString(data, encoding)
    }
    return data
}

/**
 * 
 * @param  {Uint8Array|string} data 
 * @param {SupportedEncodings|undefined} encoding
 * @returns { string }
 */
export const u8ToString = (data, encoding) => {
    if (typeof data == 'string') {
        return data
    }
    return uint8arraysToString(data, encoding)
}