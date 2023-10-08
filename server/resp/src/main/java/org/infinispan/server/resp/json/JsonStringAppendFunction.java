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
import com.fasterxml.jackson.databind.node.TextNode;
import com.jayway.jsonpath.JsonPath;

@ProtoTypeId(ProtoStreamTypeIds.RESP_JSON_STRING_APPEND_FUNCTION)
public class JsonStringAppendFunction implements SerializableFunction<ReadWriteEntryView<byte[], JsonBucket>, List<Long>> {

    @ProtoField(1)
    final byte[] path;

    @ProtoField(2)
    final byte[] value;

    @ProtoFactory
    public JsonStringAppendFunction(byte[] path, byte[] value) {
        this.path = path;
        this.value = value;
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
            var textNode = JSONUtil.objectMapper.readTree(value);
            for (JsonNode pathAsNode : pathList) {
                String asText = pathAsNode.asText();
                ArrayNode node = jpCtx.read(asText);
                if (node.get(0).isTextual()) {
                    if (JSONUtil.isRoot(asText.getBytes(StandardCharsets.UTF_8))) {
                        String newString = "\"" + node.get(0).textValue() + textNode.textValue() + "\"";
                        entryView.set(new JsonBucket(newString.getBytes(StandardCharsets.UTF_8)));
                        resList.add((long) newString.length() - 2);
                        // changed root, returning
                        return resList;
                    }
                    String newString = node.get(0).textValue() + textNode.textValue();
                    jpCtx.set(asText, TextNode.valueOf(newString));
                    changed = true;
                    resList.add((long) newString.length());
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
