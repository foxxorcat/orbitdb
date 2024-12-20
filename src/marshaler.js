import { goJSONReplacer, goJSONReviver, toUint8Array, u8ToString } from './utils/go-json.js';
import { base64 } from "multiformats/bases/base64";

const marshal = (msg, address, ver = 'js-v2') => {
    if (ver == 'go-v1') {
        // go v1
        return toUint8Array(JSON.stringify(msg, goJSONReplacer))
    }
    // js-v2
    return msg
}
const unmarshal = (payload, address, ver = 'js-v2') => {
    if (ver == 'go-v1') {
        const msg = JSON.parse(u8ToString(payload), goJSONReviver)
        for (const head of msg.heads) {
            // goJSONReviver 有概率会出错, 这里手动还原
            if (head.identity.id instanceof Uint8Array) {
                head.identity.id = base64.baseEncode(head.identity.id)
            }
            if (head.id instanceof Uint8Array) {
                head.id = base64.baseEncode(head.id)
            }
        }
        return msg
    }
    // js v2
    return bytes
}

export default { marshal, unmarshal }