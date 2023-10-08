package org.infinispan.server.resp.json;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;

import com.fasterxml.jackson.databind.JsonNode;

@ProtoTypeId(ProtoStreamTypeIds.RESP_JSON_LEN_OBJ_FUNCTION)
public class JsonLenObjFunction extends JsonLenFunction {
   @ProtoFactory
   public JsonLenObjFunction(byte[] path) {
      super(path, JsonNode::isObject, jsonNode -> (long) jsonNode.size());
   }
}
