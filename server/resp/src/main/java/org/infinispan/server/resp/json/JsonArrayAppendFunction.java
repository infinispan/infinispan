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

@ProtoTypeId(ProtoStreamTypeIds.RESP_JSON_ARRAY_APPEND_FUNCTION)
public class JsonArrayAppendFunction implements SerializableFunction<ReadWriteEntryView<byte[], JsonBucket>, List<Long>> {

    @ProtoField(1)
    final byte[] path;

    @ProtoField(2)
    final List<byte[]> values;

    @ProtoFactory
    public JsonArrayAppendFunction(byte[] path, List<byte[]> values) {
        this.path = path;
        this.values = values;
    }

    @Override
    public List<Long> apply(ReadWriteEntryView<byte[], JsonBucket> entryView) {
        var doc = entryView.find().orElse(null);
        if (doc == null) {
            throw new CacheException("could not perform this operation on a key that doesn't exist");
        }
        var pathStr = new String(path, StandardCharsets.UTF_8);
        try {
            var rootNode = JSONUtil.objectMapper.readTree(doc.value());
            var jpCtxPath = JSONUtil.parserForMod.parse(rootNode);
            var jpCtx = JSONUtil.parserForGet.parse(rootNode);
            JsonPath jpath = JsonPath.compile(pathStr);
            ArrayNode pathList = jpCtxPath.read(jpath);
            List<Long> resList = new ArrayList<>();
            boolean changed = false;
            ArrayNode srcNodes = JSONUtil.objectMapper.createArrayNode();
            for (byte[] value : values) {
                srcNodes.add(JSONUtil.objectMapper.readTree(value));
            }
            for (JsonNode pathAsNode : pathList) {
                ArrayNode node = jpCtx.read(pathAsNode.asText());
                if (node.get(0).isArray()) {
                    ArrayNode destNode = (ArrayNode) node.get(0);
                    ArrayNode newString = destNode.addAll(srcNodes);
                    if (JSONUtil.isRoot(pathAsNode.asText().getBytes(StandardCharsets.UTF_8))) {
                        // Updating the root node by replacing the cache entry
                        entryView.set(new JsonBucket(JSONUtil.objectMapper.writeValueAsBytes(newString)));
                        resList.add((long) newString.size());
                        // changed root, returning
                        return resList;
                    }
                    jpCtx.set(pathAsNode.asText(), newString);
                    changed = true;
                    resList.add((long) newString.size());
                } else {
                    resList.add(null);
                }
            }
            if (changed) {
                entryView.set(new JsonBucket(JSONUtil.objectMapper.writeValueAsBytes(rootNode)));
            }
            return resList;
        } catch (IOException e) {
            throw new CacheException(e);
        }
    }
}
