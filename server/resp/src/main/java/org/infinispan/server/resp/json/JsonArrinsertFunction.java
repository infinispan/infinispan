package org.infinispan.server.resp.json;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.server.resp.ExternalizerIds;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.util.function.SerializableFunction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.jayway.jsonpath.JsonPath;

public class JsonArrinsertFunction
        implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], JsonBucket>, List<Integer>> {
    public static final AdvancedExternalizer<JsonArrinsertFunction> EXTERNALIZER = new JsonArrinsertFunction.Externalizer();

    private byte[] path;
    private int index;
    private List<byte[]> values;

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

    private static class Externalizer implements AdvancedExternalizer<JsonArrinsertFunction> {

        @Override
        public void writeObject(ObjectOutput output, JsonArrinsertFunction object) throws IOException {
            JSONUtil.writeBytes(output, object.path);
            output.writeInt(object.index);
            output.writeInt(object.values.size());
            for (byte[] value : object.values) {
                JSONUtil.writeBytes(output, value);
            }
        }

        @Override
        public JsonArrinsertFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            byte[] jsonPath = JSONUtil.readBytes(input);
            int index = input.readInt();
            int size = input.readInt();
            List<byte[]> values = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                values.add(JSONUtil.readBytes(input));
            }
            return new JsonArrinsertFunction(jsonPath, index, values);
        }

        @Override
        public Set<Class<? extends JsonArrinsertFunction>> getTypeClasses() {
            return Collections.singleton(JsonArrinsertFunction.class);
        }

        @Override
        public Integer getId() {
            return ExternalizerIds.JSON_ARRINSERT_FUNCTION;
        }
    }
}
