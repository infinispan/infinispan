package org.infinispan.server.resp.json;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoTypeId;

@Proto
@ProtoTypeId(ProtoStreamTypeIds.RESP_JSON_LEN_TYPE)
public enum LenType {
    OBJECT, STRING, ARRAY, UNKNOWN;

    public static LenType fromCommand(String command) {
        if (command.contains("ARR")) {
            return ARRAY;
        }
        if (command.contains("OBJ")) {
            return OBJECT;
        }
        if (command.contains("STR")) {
            return STRING;
        }
        return UNKNOWN;
    }
}
