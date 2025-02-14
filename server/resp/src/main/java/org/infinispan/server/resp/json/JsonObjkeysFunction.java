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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonObjkeysFunction
        implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], JsonBucket>, List<List<byte[]>>> {
    public static final AdvancedExternalizer<JsonObjkeysFunction> EXTERNALIZER = new JsonObjkeysFunction.Externalizer();

    private byte[] path;

    public JsonObjkeysFunction(byte[] path) {
        this.path = path;
    }

    @Override
    public List<List<byte[]>> apply(ReadWriteEntryView<byte[], JsonBucket> entryView) {
        Optional<JsonBucket> existing = entryView.peek();
        if (existing.isEmpty())
            return null;
        byte[] doc = existing.get().value();
        ObjectMapper mapper = JSONUtil.objectMapper;
        JsonNode rootNode;
        try {
            rootNode = mapper.readTree(RespUtil.utf8(doc));
            var jpCtx = JSONUtil.parserForGet.parse(rootNode);
            ArrayNode nodeList = jpCtx.read(RespUtil.utf8(path));
            ArrayList<List<byte[]>> resultList = new ArrayList<>();
            for (JsonNode jsonNode : nodeList) {
                if (jsonNode.isObject()) {
                    var objNode = (ObjectNode) jsonNode;
                    List<byte[]> namesList = new ArrayList<>();
                    var iterator = objNode.fieldNames();
                    iterator.forEachRemaining((k) -> namesList.add(k.getBytes(StandardCharsets.UTF_8)));
                    resultList.add(namesList);
                } else {
                    resultList.add(null);
                }
            }
            return resultList;
        } catch (JsonProcessingException e) {
            throw new CacheException(e);
        }
    }

    private static class Externalizer implements AdvancedExternalizer<JsonObjkeysFunction> {

        @Override
        public void writeObject(ObjectOutput output, JsonObjkeysFunction object) throws IOException {
            JSONUtil.writeBytes(output, object.path);
        }

        @Override
        public JsonObjkeysFunction readObject(ObjectInput input) throws IOException {
            byte[] path = JSONUtil.readBytes(input);
            return new JsonObjkeysFunction(path);
        }

        @Override
        public Set<Class<? extends JsonObjkeysFunction>> getTypeClasses() {
            return Collections.singleton(JsonObjkeysFunction.class);
        }

        @Override
        public Integer getId() {
            return ExternalizerIds.JSON_OBJKEYS_FUNCTION;
        }
    }
}
