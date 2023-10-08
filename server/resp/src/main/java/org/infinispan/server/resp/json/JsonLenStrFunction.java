package org.infinispan.server.resp.json;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;

import com.fasterxml.jackson.databind.JsonNode;

@ProtoTypeId(ProtoStreamTypeIds.RESP_JSON_LEN_STRING_FUNCTION)
public class JsonLenStrFunction extends JsonLenFunction {
   @ProtoFactory
   public JsonLenStrFunction(byte[] path) {
      super(path, JsonNode::isTextual, jsonNode -> (long) jsonNode.asText().length());
   }
}
