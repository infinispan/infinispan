package org.infinispan.server.resp.json;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;

import com.fasterxml.jackson.databind.JsonNode;

@ProtoTypeId(ProtoStreamTypeIds.RESP_JSON_LEN_ARRAY_FUNCTION)
public class JsonLenArrayFunction extends JsonLenFunction {
   @ProtoFactory
   public JsonLenArrayFunction(byte[] path) {
     super(path, JsonNode::isArray, jsonNode -> (long) jsonNode.size());
   }
}
