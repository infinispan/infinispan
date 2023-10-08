package org.infinispan.server.resp.json;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.server.resp.serialization.RespConstants;
import org.infinispan.util.function.SerializableFunction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

@ProtoTypeId(ProtoStreamTypeIds.RESP_JSON_MERGE_FUNCTION)
public class JsonMergeFunction implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], JsonBucket>, String> {

    @ProtoField(1)
    final byte[] path;
    @ProtoField(2)
    final byte[] value;

    @ProtoFactory
    public JsonMergeFunction(byte[] path, byte[] value) {
        this.path = path;
        this.value = value;
    }

    @Override
    public String apply(ReadWriteEntryView<byte[], JsonBucket> entryView) {
        var doc = (JsonBucket) entryView.find().orElse(null);
        if (doc == null && !JSONUtil.isRoot(path)) {
            throw new CacheException("new objects must be created at root");
        }
        try {
            JsonNode updateNode = JSONUtil.objectMapper.readTree(value);
            if (doc == null) {
                entryView.set(new JsonBucket(value));
                return RespConstants.OK;
            } else {
                var rootObjectNode = (ObjectNode) JSONUtil.objectMapper.readTree(doc.value());
                if (JSONUtil.isRoot(path)) {
                    return handleRootMerge(entryView, updateNode, rootObjectNode);
                } else {
                    return handlePathMerge(entryView, updateNode, rootObjectNode);
                }
            }
        } catch (IOException e) {
            throw new CacheException(e);
        }
    }

    private String handlePathMerge(ReadWriteEntryView<byte[], JsonBucket> entryView, JsonNode updateNode,
                                   ObjectNode rootObjectNode)
            throws JsonProcessingException {
        var pathStr = new String(path, StandardCharsets.UTF_8);
        JsonPath jpath = JsonPath.compile(pathStr);
        var jpCtxPath = JSONUtil.parserForMod.parse(rootObjectNode);
        var jpCtx = JSONUtil.parserForDefiniteSet.parse(rootObjectNode);
        if (jpath.isDefinite()) {
            return handleDefinitePath(entryView, updateNode, rootObjectNode, jpath, jpCtx);
        }
        return handleWildcardPath(entryView, updateNode, rootObjectNode, jpath, jpCtxPath, jpCtx);
    }

    private String handleWildcardPath(ReadWriteEntryView<byte[], JsonBucket> entryView, JsonNode updateNode,
                               ObjectNode rootObjectNode, JsonPath jpath, DocumentContext jpCtxPath,
                               DocumentContext jpCtx)
            throws JsonProcessingException {
        ArrayNode pathList = jpCtxPath.read(jpath);
        // If path matches nothing, return error
        if (pathList.size() == 0) {
            throw new CacheException("Err wrong static path");
        } else {
            for (JsonNode pathAsNode : pathList) {
                merge(updateNode, JsonPath.compile(pathAsNode.asText()), jpCtx);
            }
            entryView.set(new JsonBucket(JSONUtil.objectMapper.writeValueAsBytes(rootObjectNode)));
            return RespConstants.OK;
        }
    }

    private String handleDefinitePath(ReadWriteEntryView<byte[], JsonBucket> entryView, JsonNode updateNode,
                             ObjectNode rootObjectNode, JsonPath jpath, DocumentContext jpCtx) {
        try {
            // JsonPath doesn't provide a method to get the parent path, so we need to
            // rely on read() method. If parent doesn't exist, it will throw an exception
            // if parent exists but the child doesn't, it will return null
            if (((JsonNode) jpCtx.read(jpath)).isNull()) {
                jpCtx.set(jpath, updateNode);
            } else {
                merge(updateNode, jpath, jpCtx);
            }
            entryView.set(new JsonBucket(JSONUtil.objectMapper.writeValueAsBytes(rootObjectNode)));
            return RespConstants.OK;
        } catch (Exception e) {
            return null;
        }
    }

    private String handleRootMerge(ReadWriteEntryView<byte[], JsonBucket> entryView, JsonNode updateNode,
                                   ObjectNode rootObjectNode)
            throws JsonProcessingException {
        if (updateNode.isNull()) {
            entryView.set(new JsonBucket(JSONUtil.objectMapper.writeValueAsBytes(updateNode)));
        } else {
            deepMerge((ObjectNode) rootObjectNode, (ObjectNode) updateNode);
            entryView.set(new JsonBucket(JSONUtil.objectMapper.writeValueAsBytes(rootObjectNode)));
        }
        return RespConstants.OK;
    }

    private void merge(JsonNode updateNode, JsonPath jpath, DocumentContext jpCtx) {
        JsonNode jn = jpCtx.read(jpath);
        if (jn == null) {
            return;
        }
        if (jn.isObject() && updateNode.isObject()) {
            deepMerge((ObjectNode) jn, (ObjectNode) updateNode);
        } else {
            jpCtx.set(jpath, updateNode);
        }
    }

    private static void deepMerge(ObjectNode target, ObjectNode source) {
        source.fields().forEachRemaining(entry -> {
            String fieldName = entry.getKey();
            JsonNode sourceValue = entry.getValue();
            JsonNode targetValue = target.get(fieldName);

            // If target has the same field and both are objects, merge recursively
            if (targetValue != null && targetValue.isObject() && sourceValue.isObject()) {
                deepMerge((ObjectNode) targetValue, (ObjectNode) sourceValue);
            } else {
                // Otherwise, override target with source value
                if (sourceValue.isNull()) {
                    target.remove(fieldName);
                } else {
                    target.set(fieldName, sourceValue);
                }
            }
        });
    }
}
