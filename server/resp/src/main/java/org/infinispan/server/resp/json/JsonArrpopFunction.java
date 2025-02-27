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

public class JsonArrpopFunction implements SerializableFunction<ReadWriteEntryView<byte[], JsonBucket>, List<byte[]>> {
    public static final AdvancedExternalizer<JsonArrpopFunction> EXTERNALIZER = new JsonArrpopFunction.Externalizer();
    private byte[] path;
    private int index;

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

    public static class Externalizer implements AdvancedExternalizer<JsonArrpopFunction> {

        @Override
        public void writeObject(ObjectOutput output, JsonArrpopFunction object) throws IOException {
            JSONUtil.writeBytes(output, object.path);
            output.writeInt(object.index);
        }

        @Override
        public JsonArrpopFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            byte[] jsonPath = JSONUtil.readBytes(input);
            int index = input.readInt();
            return new JsonArrpopFunction(jsonPath, index);
        }

        @Override
        public Set<Class<? extends JsonArrpopFunction>> getTypeClasses() {
            return Collections.singleton(JsonArrpopFunction.class);
        }

        @Override
        public Integer getId() {
            return ExternalizerIds.JSON_ARRPOP_FUNCTION;
        }
    }
}
