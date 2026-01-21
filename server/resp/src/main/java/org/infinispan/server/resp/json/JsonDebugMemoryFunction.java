package org.infinispan.server.resp.json;

import static java.util.Objects.requireNonNull;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.marshall.core.JOLEntrySizeCalculator;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.jayway.jsonpath.JsonPath;

@ProtoTypeId(ProtoStreamTypeIds.RESP_JSON_DEBUG_MEMORY_FUNCTION)
public class JsonDebugMemoryFunction
      implements SerializableFunction<ReadWriteEntryView<byte[], JsonBucket>, List<Long>> {
   public static final String ERR_PATH_CAN_T_BE_NULL = "path can't be null";
   private static final JOLEntrySizeCalculator<?, byte[]> jol = JOLEntrySizeCalculator.getInstance();


   @ProtoField(1)
   byte[] path;

   @ProtoFactory
   public JsonDebugMemoryFunction(byte[] path) {
      requireNonNull(path, ERR_PATH_CAN_T_BE_NULL);
      this.path = path;
   }

   @Override
   public List<Long> apply(ReadWriteEntryView<byte[], JsonBucket> entryView) {
      var doc = entryView.find().orElse(null);
      if (doc == null) {
        return List.of(0L);
      }

      var pathStr = new String(path, StandardCharsets.UTF_8);
      try {
         if (JSONUtil.isRoot(path)) {
            return List.of(jol.deepSizeOf(doc.value()));
         }
         var rootNode = JSONUtil.objectMapper.readTree(doc.value());
         var jpCtx = JSONUtil.parserForGet.parse(rootNode);
         JsonPath jpath = JsonPath.compile(pathStr);
         ArrayNode nodeList = jpCtx.read(jpath);
         List<Long> resList = new ArrayList<>(nodeList.size());
         for (JsonNode jsonNode : nodeList) {
            resList.add(Long.valueOf(jsonNode.toString().getBytes(StandardCharsets.UTF_8).length));
         }
         return resList;
      } catch (CacheException e) {
         throw e;
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }
}
