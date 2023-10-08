package org.infinispan.server.resp.json;

import static java.util.Objects.requireNonNull;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

@ProtoTypeId(ProtoStreamTypeIds.RESP_JSON_TOGGLE_FUNCTION)
public class JsonToggleFunction implements SerializableFunction<ReadWriteEntryView<byte[], JsonBucket>, List<Integer>> {
   public static final String ERR_PATH_CAN_T_BE_NULL = "path can't be null";

   @ProtoField(1)
   final byte[] path;

   @ProtoFactory
   public JsonToggleFunction(byte[] path) {
      requireNonNull(path, ERR_PATH_CAN_T_BE_NULL);
      this.path = path;
   }

   @Override
   public List<Integer> apply(ReadWriteEntryView<byte[], JsonBucket> entryView) {
      var doc = entryView.find().orElse(null);
      if (doc == null) {
         throw new CacheException("could not perform this operation on a key that doesn't exist");
      }

      var pathStr = new String(path, StandardCharsets.UTF_8);
      try {
         JsonNode rootNode = JSONUtil.objectMapper.readTree(doc.value());
         DocumentContext modifiableCtx = JSONUtil.parserForMod.parse(rootNode);
         DocumentContext getForContext = JSONUtil.parserForGet.parse(rootNode);
         JsonPath jpath = JsonPath.compile(pathStr);
         ArrayNode pathList = modifiableCtx.read(jpath);
         boolean changed = false;
         List<Integer> resList = new ArrayList<>(pathList.size());
         for (JsonNode pathAsNode : pathList) {
            String pathAsText = pathAsNode.asText();
            ArrayNode node = getForContext.read(pathAsText);
            if (node.get(0).isBoolean()) {
               boolean currentBolValue = node.get(0).asBoolean();
               resList.add(currentBolValue? 0 : 1);
               if (JSONUtil.isRoot(pathAsText.getBytes(StandardCharsets.UTF_8))) {
                  entryView.set(new JsonBucket(Boolean.toString(currentBolValue ? false : true).getBytes(StandardCharsets.UTF_8)));
                  // changed root, returning
                  return resList;
               }
               modifiableCtx.set(pathAsText, currentBolValue? BooleanNode.FALSE : BooleanNode.TRUE);
               changed = true;
            } else {
               resList.add(null);
            }
         }
         if (changed) {
            entryView.set(new JsonBucket(JSONUtil.objectMapper.writeValueAsBytes(rootNode)));
         }
         return resList;
      } catch (CacheException e) {
         throw e;
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }
}
