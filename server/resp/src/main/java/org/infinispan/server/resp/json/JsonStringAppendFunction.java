package org.infinispan.server.resp.json;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.server.resp.ExternalizerIds;
import org.infinispan.util.function.SerializableFunction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.jayway.jsonpath.JsonPath;

public class JsonStringAppendFunction
        implements SerializableFunction<ReadWriteEntryView<byte[], JsonBucket>, List<Long>> {
    public static final AdvancedExternalizer<JsonStringAppendFunction> EXTERNALIZER = new JsonStringAppendFunction.Externalizer();
    final byte[] path;
    final byte[] value;

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

    private static class Externalizer implements AdvancedExternalizer<JsonStringAppendFunction> {

        @Override
        public void writeObject(ObjectOutput output, JsonStringAppendFunction jsonAppendFunction) throws IOException {
            JSONUtil.writeBytes(output, jsonAppendFunction.path);
            JSONUtil.writeBytes(output, jsonAppendFunction.value);
        }

        @Override
        public JsonStringAppendFunction readObject(ObjectInput input) throws IOException {
            byte[] path1 = JSONUtil.readBytes(input);
            byte[] value = JSONUtil.readBytes(input);
            return new JsonStringAppendFunction(path1, value);
        }

        @Override
        public Set<Class<? extends JsonStringAppendFunction>> getTypeClasses() {
            return Collections.singleton(JsonStringAppendFunction.class);
        }

        @Override
        public Integer getId() {
            return ExternalizerIds.JSON_STRING_APPEND_FUNCTION;
        }
    }
}
