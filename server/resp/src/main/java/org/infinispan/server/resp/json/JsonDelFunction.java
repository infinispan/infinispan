package org.infinispan.server.resp.json;

import static java.util.Objects.requireNonNull;

import java.nio.charset.StandardCharsets;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.jayway.jsonpath.JsonPath;

@ProtoTypeId(ProtoStreamTypeIds.RESP_JSON_DEL_FUNCTION)
public class JsonDelFunction
      implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], JsonBucket>, Long> {
   public static final String ERR_PATH_CAN_T_BE_NULL = "path can't be null";

   @ProtoField(1)
   final byte[] path;

   @ProtoFactory
   public JsonDelFunction(byte[] path) {
      requireNonNull(path, ERR_PATH_CAN_T_BE_NULL);
      this.path = path;
   }

   @Override
   public Long apply(ReadWriteEntryView<byte[], JsonBucket> entryView) {
      var doc = entryView.find().orElse(null);
      if (doc == null) {
         return 0L;
      }
      byte[] jsonPath = JSONUtil.toJsonPath(path);
      try {
         JsonNode rootObjectNode = JSONUtil.objectMapper.readTree(doc.value());
         var jpCtx = JSONUtil.parserForGet.parse(rootObjectNode);
         if (JSONUtil.isRoot(jsonPath)) {
            entryView.remove();
            return 1L;
         }
         var pathStr = new String(jsonPath, StandardCharsets.UTF_8);
         JsonPath jpath = JsonPath.compile(pathStr);
         ArrayNode an = jpCtx.read(jpath);
         if (an.size() == 0 || an.size() == 1 && an.get(0) == null) {
            return 0L;
         }
         jpCtx = jpCtx.delete(jpath);
         entryView.set(new JsonBucket(JSONUtil.objectMapper.writeValueAsBytes(rootObjectNode)));
         return (long) an.size();
      } catch (IndexOutOfBoundsException e) {
         // Trying to delete a non existent index of an array return 0
         return 0L;
      } catch (CacheException e) {
         throw e;
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }
}
