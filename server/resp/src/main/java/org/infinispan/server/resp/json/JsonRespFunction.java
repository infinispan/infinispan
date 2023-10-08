package org.infinispan.server.resp.json;

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

@ProtoTypeId(ProtoStreamTypeIds.RESP_JSON_FUNCTION)
public class JsonRespFunction implements SerializableFunction<ReadWriteEntryView<byte[], JsonBucket>, List<Object>> {

    @ProtoField(1)
    final byte[] path;

    @ProtoFactory
    public JsonRespFunction(byte[] path) {
        this.path = path;
    }

    @Override
    public List<Object> apply(ReadWriteEntryView<byte[], JsonBucket> entryView) {
        var doc = entryView.find().orElse(null);
        if (doc == null) {
            return null;
        }
        var pathStr = new String(path, StandardCharsets.UTF_8);
        try {
            var rootNode = JSONUtil.objectMapper.readTree(doc.value());
            var jpCtx = JSONUtil.parserForGet.parse(rootNode);
            JsonPath jpath = JsonPath.compile(pathStr);
            ArrayNode nodeList = jpCtx.read(jpath);
            List<Object> result = new ArrayList<>();
            for (JsonNode jsonNode : nodeList) {
                result.add(resp(jsonNode));
            }
            return result;
        } catch (CacheException e) {
            throw e;
        } catch (Exception e) {
            throw new CacheException(e);
        }
    }

    private Object resp(JsonNode jsonNode) {
        if (jsonNode.isArray()) {
            ArrayList<Object> arr = new ArrayList<>();
            arr.add("[");
            for (JsonNode jsonNode2 : ((ArrayNode) jsonNode)) {
                arr.add(resp(jsonNode2));
            }
            return arr;
        }
        if (jsonNode.isObject()) {
            ArrayList<Object> obj = new ArrayList<>();
            obj.add("{");
            jsonNode.fields().forEachRemaining(entry -> {
                obj.add(entry.getKey());
                obj.add(resp(entry.getValue()));
            });
            return obj;
        }
        if (jsonNode.isInt()) {
            return jsonNode.asInt();
        }
        if (jsonNode.isLong()) {
            return jsonNode.asLong();
        }
        if (jsonNode.isFloat() || jsonNode.isDouble()) {
            return jsonNode.asDouble();
        }
        if (jsonNode.isBoolean()) {
            return jsonNode.asBoolean();
        }
        if (jsonNode.isTextual()) {
            return jsonNode.asText();
        }
        if (jsonNode.isNull()) {
            return null;
        }
        return jsonNode.toString();
    }
}
