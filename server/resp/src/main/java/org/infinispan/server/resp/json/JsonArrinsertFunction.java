package org.infinispan.server.resp.json;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.util.function.SerializableFunction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.jayway.jsonpath.JsonPath;

@ProtoTypeId(ProtoStreamTypeIds.RESP_JSON_ARRINSERT_FUNCTION)
public class JsonArrinsertFunction
        implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], JsonBucket>, List<Integer>> {

    @ProtoField(1)
    final byte[] path;

    @ProtoField(2)
    final int index;

    @ProtoField(3)
    final List<byte[]> values;

    @ProtoFactory
    public JsonArrinsertFunction(byte[] path, int index, List<byte[]> values) {
        this.path = path;
        this.index = index;
        this.values = values;
    }

    @Override
    public List<Integer> apply(ReadWriteEntryView<byte[], JsonBucket> entryView) {
        Optional<JsonBucket> existing = entryView.peek();
        if (existing.isEmpty()) {
            return null;
        }
        byte[] doc = existing.get().value();
        ObjectMapper mapper = JSONUtil.objectMapper;
        try {
            JsonNode rootNode = mapper.readTree(RespUtil.utf8(doc));
            ArrayNode srcNodes = JSONUtil.objectMapper.createArrayNode();
            for (byte[] value : values) {
                srcNodes.add(JSONUtil.objectMapper.readTree(value));
            }
            var jpCtxPath = JSONUtil.parserForMod.parse(rootNode);
            var jpCtx = JSONUtil.parserForGet.parse(rootNode);
            JsonPath jpath = JsonPath.compile(RespUtil.utf8(path));
            ArrayNode pathList = jpCtxPath.read(jpath);
            List<Integer> resList = new ArrayList<>();
            boolean changed = false;
            for (JsonNode pathAsNode : pathList) {
                String asText = pathAsNode.asText();
                ArrayNode node = jpCtx.read(asText);
                if (node.get(0).isArray()) {
                    ArrayNode destNode = (ArrayNode) node.get(0);
                    int index = this.index > 0 ? this.index : destNode.size() + this.index;
                    ArrayNode newArray = insertAll(destNode, srcNodes, index);
                    if (JSONUtil.isRoot(asText.getBytes(StandardCharsets.UTF_8))) {
                        // Updating the root node by replacing the cache entry
                        entryView.set(new JsonBucket(JSONUtil.objectMapper.writeValueAsBytes(newArray)));
                        resList.add(newArray.size());
                        // changed root, returning
                        return resList;
                    }
                    jpCtx.set(asText, newArray);
                    changed = true;
                    resList.add(newArray.size());
                } else {
                    resList.add(null);
                }
                if (changed) {
                    entryView.set(new JsonBucket(JSONUtil.objectMapper.writeValueAsBytes(rootNode)));
                }
            }
            return resList;
        } catch (IOException e) {
            throw new CacheException(e);
        }

    }

    private ArrayNode insertAll(ArrayNode outer, ArrayNode inner, int index) {
        if (index > outer.size() || index < 0) {
            throw new CacheException("index out of bounds");
        }
        ArrayNode result = JSONUtil.objectMapper.createArrayNode();
        for (int i = 0; i < index; i++) {
            result.add(outer.get(i));
        }
        result.addAll(inner);
        for (int i = index; i < outer.size(); i++) {
            result.add(outer.get(i));
        }
        return result;
    }
}
