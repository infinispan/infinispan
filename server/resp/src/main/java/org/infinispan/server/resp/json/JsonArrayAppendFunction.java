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
import com.jayway.jsonpath.JsonPath;

public class JsonArrayAppendFunction
        implements SerializableFunction<ReadWriteEntryView<byte[], JsonBucket>, List<Long>> {
    public static final AdvancedExternalizer<JsonArrayAppendFunction> EXTERNALIZER = new JsonArrayAppendFunction.Externalizer();
    final byte[] path;
    final List<byte[]> arrValues;

    public JsonArrayAppendFunction(byte[] path, List<byte[]> values) {
        this.path = path;
        this.arrValues = values;
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
            for (byte[] value : arrValues) {
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

    private static class Externalizer implements AdvancedExternalizer<JsonArrayAppendFunction> {

        @Override
        public void writeObject(ObjectOutput output, JsonArrayAppendFunction jsonAppendFunction) throws IOException {
            JSONUtil.writeBytes(output, jsonAppendFunction.path);
            output.writeInt(jsonAppendFunction.arrValues.size());
            for (byte[] value : jsonAppendFunction.arrValues) {
                JSONUtil.writeBytes(output, value);
            }
        }

        @Override
        public JsonArrayAppendFunction readObject(ObjectInput input) throws IOException {
            byte[] path = JSONUtil.readBytes(input);
            int count = input.readInt();
            var values = new ArrayList<byte[]>();
            for (int i = 0; i < count; i++) {
                values.add(JSONUtil.readBytes(input));
            }
            return new JsonArrayAppendFunction(path, values);
        }

        @Override
        public Set<Class<? extends JsonArrayAppendFunction>> getTypeClasses() {
            return Collections.singleton(JsonArrayAppendFunction.class);
        }

        @Override
        public Integer getId() {
            return ExternalizerIds.JSON_ARRAY_APPEND_FUNCTION;
        }
    }
}
