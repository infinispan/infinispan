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
import org.infinispan.functional.EntryView;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.server.resp.ExternalizerIds;
import org.infinispan.util.function.SerializableFunction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.jayway.jsonpath.JsonPath;

public class JsonArrtrimFunction
        implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], JsonBucket>, List<Integer>> {

    public static final AdvancedExternalizer<JsonArrtrimFunction> EXTERNALIZER = new JsonArrtrimFunction.Externalizer();
    private byte[] path;
    private int start;
    private int stop;

    public JsonArrtrimFunction(byte[] path, int start, int stop) {
        this.path = path;
        this.start = start;
        this.stop = stop;
    }

    @Override
    public List<Integer> apply(ReadWriteEntryView<byte[], JsonBucket> entryView) {
        var doc = entryView.find().orElse(null);
        if (doc == null) {
            return null;
        }
        var pathStr = new String(path, StandardCharsets.UTF_8);
        try {
            var rootNode = JSONUtil.objectMapper.readTree(doc.value());
            var jpCtxPath = JSONUtil.parserForMod.parse(rootNode);
            var jpCtx = JSONUtil.parserForGet.parse(rootNode);
            JsonPath jpath = JsonPath.compile(pathStr);
            ArrayNode pathList = jpCtxPath.read(jpath);
            List<Integer> resList = new ArrayList<>();
            boolean changed = false;
            for (JsonNode pathAsNode : pathList) {
                ArrayNode node = jpCtx.read(pathAsNode.asText());
                if (node.get(0).isArray()) {
                    ArrayNode destNode = (ArrayNode) node.get(0);
                    int start = toTrimStartIndex(this.start, destNode.size());
                    int stop = toTrimStopIndex(this.stop, destNode.size());
                    if (start > stop || start >= destNode.size()) {
                        if (destNode.size() > 0) {
                            destNode.removeAll();
                            changed = true;
                        }
                    } else {
                        for (int i = destNode.size() - 1; i > stop; i--) {
                            destNode.remove(i);
                            changed = true;
                        }
                        for (int i = 0; i < start; i++) {
                            destNode.remove(0);
                            changed = true;
                        }
                    }
                    if (JSONUtil.isRoot(pathAsNode.asText().getBytes(StandardCharsets.UTF_8))) {
                        // Updating the root node by replacing the cache entry
                        if (changed) {
                            entryView.set(new JsonBucket(JSONUtil.objectMapper.writeValueAsBytes(destNode)));
                        }
                        resList.add(destNode.size());
                        // changed root, returning
                        return resList;
                    }
                    jpCtx.set(pathAsNode.asText(), destNode);
                    resList.add(destNode.size());
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

    private static int toTrimStartIndex(int start, int size) {
        if (start < 0) {
            return Math.max(size + start, 0);
        }
        return Math.min(start, size);
    }

    private static int toTrimStopIndex(int stop, int size) {
        if (stop < 0) {
            return Math.max(size + stop, 0);
        }
        return Math.min(stop, size-1);
    }

    private static class Externalizer implements AdvancedExternalizer<JsonArrtrimFunction> {

        @Override
        public void writeObject(ObjectOutput output, JsonArrtrimFunction object) throws IOException {
            JSONUtil.writeBytes(output, object.path);
            output.writeInt(object.start);
            output.writeInt(object.stop);
        }

        @Override
        public JsonArrtrimFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            byte[] jsonPath = JSONUtil.readBytes(input);
            int start = input.readInt();
            int stop = input.readInt();
            return new JsonArrtrimFunction(jsonPath, start, stop);
        }

        @Override
        public Set<Class<? extends JsonArrtrimFunction>> getTypeClasses() {
            return Collections.singleton(JsonArrtrimFunction.class);
        }

        @Override
        public Integer getId() {
            return ExternalizerIds.JSON_ARRTRIM_FUNCTION;
        }
    }

}
