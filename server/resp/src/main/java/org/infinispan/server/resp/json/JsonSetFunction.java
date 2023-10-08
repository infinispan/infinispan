package org.infinispan.server.resp.json;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.server.resp.serialization.RespConstants;
import org.infinispan.util.function.SerializableFunction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;

@ProtoTypeId(ProtoStreamTypeIds.RESP_JSON_SET_FUNCTION)
public class JsonSetFunction
      implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], JsonBucket>, String> {
   public static final String ERR_VALUE_CAN_T_BE_NULL = "value can't be null";
   public static final String ERR_PATH_CAN_T_BE_NULL = "path can't be null";

   @ProtoField(1)
   final byte[] value;
   @ProtoField(2)
   final byte[] path;
   @ProtoField(3)
   final boolean nx;
   @ProtoField(4)
   final boolean xx;

   @ProtoFactory
   public JsonSetFunction(byte[] value, byte[] path, boolean nx, boolean xx) {
      requireNonNull(value, ERR_VALUE_CAN_T_BE_NULL);
      requireNonNull(path, ERR_PATH_CAN_T_BE_NULL);
      this.value = value;
      this.path = path;
      this.nx = nx;
      this.xx = xx;
   }

   @Override
   public String apply(ReadWriteEntryView<byte[], JsonBucket> entryView) {
      JsonNode newNode;
      try {
         newNode = JSONUtil.objectMapper.readTree(value);
      } catch (IOException e) {
         throw new CacheException(e);
      }
      var doc = (JsonBucket) entryView.find().orElse(null);
      byte[] jsonPath = JSONUtil.toJsonPath(path);
      if (doc == null) {
         if (xx) {
            return null;
         }
         if (!JSONUtil.isRoot(jsonPath)) {
            throw new CacheException("new objects must be created at root");
         }
         entryView.set(new JsonBucket(value));
         return RespConstants.OK;
      }
      if (JSONUtil.isRoot(jsonPath)) {
         if (nx) {
            return null;
         }
         // Updating the root node is not allowed by jsonpath
         // replacing the whole doc here
         entryView.set(new JsonBucket(value));
         return RespConstants.OK;
      }
      try {
         var rootObjectNode = (ObjectNode) JSONUtil.objectMapper.readTree(doc.value());
         var jpCtx = JSONUtil.parserForSet.parse(rootObjectNode);
         var pathStr = new String(jsonPath, StandardCharsets.UTF_8);
         JsonNode node = jpCtx.read(pathStr);
         if ((node == null || node.isNull()) && xx || node != null && !node.isNull() && nx) {
            return null;
         }
         Object resObj;
         JsonPath jpath = JsonPath.compile(pathStr);
         if (jpath.isDefinite()) {
            resObj = jpath.set(jpCtx.json(), newNode, JSONUtil.configForDefiniteSet);
         } else {
            resObj = jpath.set(jpCtx.json(), newNode, JSONUtil.configForSet);
            if (((JsonNode)resObj).isEmpty()) {
               throw new CacheException("Wrong static path");
            }
         }
         if (resObj != null) {
            entryView.set(new JsonBucket(JSONUtil.objectMapper.writeValueAsBytes(rootObjectNode)));
            return RespConstants.OK;
         } else {
            return null;
         }
      } catch (PathNotFoundException ex) {
         // mimicking redis. Not an error, do nothing and return null
         return null;
      } catch (CacheException e) {
         throw e;
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }
}
