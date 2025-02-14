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
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.server.resp.ExternalizerIds;
import org.infinispan.util.function.SerializableFunction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.jayway.jsonpath.JsonPath;

public class JsonAppendFunction
        implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], JsonBucket>, List<Long>> {
    public static final AdvancedExternalizer<JsonAppendFunction> EXTERNALIZER = new JsonAppendFunction.Externalizer();
    public AppendType appendType;
    public byte[] path;
    public byte[] value;
    private List<byte[]> arrValues;

    public JsonAppendFunction(byte[] path, byte[] value) {
        this.appendType = AppendType.STRING;
        this.path = path;
        this.value = value;
    }

    public JsonAppendFunction(byte[] path, List<byte[]> values) {
        this.appendType = AppendType.ARRAY;
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
            switch (appendType) {
                case ARRAY:
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
                    break;
                case STRING:
                    var textNode = JSONUtil.objectMapper.readTree(value);
                    changed = false;
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
                    break;
                case UNKNOWN:
                default:
                    throw new CacheException("wrong type for this function");
            }
            if (changed) {
                entryView.set(new JsonBucket(JSONUtil.objectMapper.writeValueAsBytes(rootNode)));
            }
            return resList;
        } catch (IOException e) {
            throw new CacheException(e);
        }
    }

    private static class Externalizer implements AdvancedExternalizer<JsonAppendFunction> {

        @Override
        public void writeObject(ObjectOutput output, JsonAppendFunction jsonAppendFunction) throws IOException {
            MarshallUtil.marshallEnum(jsonAppendFunction.appendType, output);
            JSONUtil.writeBytes(output, jsonAppendFunction.path);
            switch (jsonAppendFunction.appendType) {
                case ARRAY:
                    output.writeInt(jsonAppendFunction.arrValues.size());
                    for (byte[] value : jsonAppendFunction.arrValues) {
                        JSONUtil.writeBytes(output, value);
                    }
                    break;
                case STRING:
                    JSONUtil.writeBytes(output, jsonAppendFunction.value);
                    break;
                case UNKNOWN:
                default:
                    throw new RuntimeException("wrong type for this function");
            }
        }

        @Override
        public JsonAppendFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            AppendType appType = MarshallUtil.unmarshallEnum(input, AppendType::valueOf);
            switch (appType) {
                case ARRAY:
                    byte[] path = JSONUtil.readBytes(input);
                    int count = input.readInt();
                    var values = new ArrayList<byte[]>();
                    for (int i = 0; i < count; i++) {
                        values.add(JSONUtil.readBytes(input));
                    }
                    return new JsonAppendFunction(path, values);
                case STRING:
                    byte[] path1 = JSONUtil.readBytes(input);
                    byte[] value = JSONUtil.readBytes(input);
                    return new JsonAppendFunction(path1, value);
                case UNKNOWN:
                default:
                    throw new CacheException("wrong type for this function");
            }
        }

        @Override
        public Set<Class<? extends JsonAppendFunction>> getTypeClasses() {
            return Collections.singleton(JsonAppendFunction.class);
        }

        @Override
        public Integer getId() {
            return ExternalizerIds.JSON_APPEND_FUNCTION;
        }
    }

}
