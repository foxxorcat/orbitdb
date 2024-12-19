import { goJSONReplacer, goJSONReviver, toUint8Array, u8ToString } from './utils/go-json.js';

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
        return JSON.parse(u8ToString(payload), goJSONReviver)
    }
    // js v2
    return bytes
}

export default { marshal, unmarshal }