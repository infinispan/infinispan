package org.infinispan.server.resp.json;

import java.io.IOException;
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
import com.jayway.jsonpath.JsonPath;

@ProtoTypeId(ProtoStreamTypeIds.RESP_JSON_ARRPOP_FUNCTION)
public class JsonArrpopFunction implements SerializableFunction<ReadWriteEntryView<byte[], JsonBucket>, List<byte[]>> {

    @ProtoField(1)
    final byte[] path;

    @ProtoField(2)
    final int index;

    @ProtoFactory
    public JsonArrpopFunction(byte[] path, int index) {
        this.path = path;
        this.index = index;
    }

    @Override
    public List<byte[]> apply(ReadWriteEntryView<byte[], JsonBucket> entryView) {
        var doc = entryView.find().orElse(null);
        var pathStr = new String(path, StandardCharsets.UTF_8);
        if (doc == null) {
            return null;
        }
        try {
            var rootNode = JSONUtil.objectMapper.readTree(doc.value());
            var jpCtxPath = JSONUtil.parserForMod.parse(rootNode);
            var jpCtx = JSONUtil.parserForGet.parse(rootNode);
            JsonPath jpath = JsonPath.compile(pathStr);
            ArrayNode pathList = jpCtxPath.read(jpath);
            List<byte[]> resList = new ArrayList<>();
            boolean changed = false;
            for (JsonNode pathAsNode : pathList) {
                ArrayNode node = jpCtx.read(pathAsNode.asText());
                JsonNode jsonNode = node.get(0);
                if (jsonNode.isArray() && jsonNode.size() > 0) {
                    ArrayNode destNode = (ArrayNode) jsonNode;
                    int removeIndex = toRemoveIndex(index, destNode.size());
                    JsonNode removed = destNode.remove(removeIndex);
                    if (JSONUtil.isRoot(pathAsNode.asText().getBytes(StandardCharsets.UTF_8))) {
                        // Updating the root node by replacing the cache entry
                        entryView.set(new JsonBucket(JSONUtil.objectMapper.writeValueAsBytes(destNode)));
                        resList.add((removed.toString().getBytes(StandardCharsets.UTF_8)));
                    } else {
                        jpCtx.set(pathAsNode.asText(), destNode);
                        changed = true;
                        resList.add((removed.toString().getBytes(StandardCharsets.UTF_8)));
                    }
                } else {
                    resList.add(null);
                }
            }
            if (changed) {
                entryView.set(new JsonBucket(JSONUtil.objectMapper.writeValueAsBytes(rootNode)));
            }
            return resList;
        } catch (

        IOException e) {
            throw new CacheException(e);
        }
    }

    private int toRemoveIndex(int index, int size) {
        return index < 0 ? Math.max(0, size + index) : Math.min(size - 1, index);
    }
}
