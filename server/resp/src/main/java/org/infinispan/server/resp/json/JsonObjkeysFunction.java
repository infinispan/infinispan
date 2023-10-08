package org.infinispan.server.resp.json;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.util.function.SerializableFunction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@ProtoTypeId(ProtoStreamTypeIds.RESP_JSON_OBJ_KEY_FUNCTION)
public class JsonObjkeysFunction
        implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], JsonBucket>, List<List<byte[]>>> {

    @ProtoField(1)
    final byte[] path;

    @ProtoFactory
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
}
