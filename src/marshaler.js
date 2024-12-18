import { goJSONReplacer, goJSONReviver, toUint8Array, u8ToString } from './utils/go-json.js';

const builderMarshaler = (ver) => {
    const marshal = (msg) => {
        if (ver == 'go-v1') {
            // go v1
            return toUint8Array(JSON.stringify(msg, goJSONReplacer))
        }
        // js-v2
        return msg
    }
    const unmarshal = (payload) => {
        if (ver == 'go-v1') {
            return JSON.parse(u8ToString(payload), goJSONReviver)
        }
        // js v2
        return payload
    }
    return { marshal, unmarshal }
}

export default builderMarshaler