package org.infinispan.server.resp.json;

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

@ProtoTypeId(ProtoStreamTypeIds.RESP_JSON_ARRINDEX_FUNCTION)
public class JsonArrindexFunction
        implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], JsonBucket>, List<Integer>> {

    @ProtoField(1)
    final byte[] jsonPath;

    @ProtoField(2)
    final byte[] value;

    @ProtoField(3)
    final int start;

    @ProtoField(4)
    final int stop;

    @ProtoField(5)
    final boolean isLegacy;

    @ProtoFactory
    public JsonArrindexFunction(byte[] jsonPath, byte[] value, int start, int stop, boolean isLegacy) {
        this.jsonPath = jsonPath;
        this.value = value;
        this.start = start;
        this.stop = stop;
        this.isLegacy = isLegacy;
    }

    @Override
    public List<Integer> apply(ReadWriteEntryView<byte[], JsonBucket> entryView) {
        Optional<JsonBucket> existing = entryView.peek();
        if (existing.isEmpty()) {
            throw new CacheException("Path '" + RespUtil.ascii(jsonPath) + "' does not exist");
        }
        byte[] doc = existing.get().value();
        ObjectMapper mapper = JSONUtil.objectMapper;
        try {
            JsonNode rootNode = mapper.readTree(RespUtil.utf8(doc));
            JsonNode valueNode = mapper.readTree(RespUtil.utf8(value));
            var jpCtx = JSONUtil.parserForGet.parse(rootNode);
            ArrayNode nodeList = jpCtx.read(RespUtil.utf8(jsonPath));
            List<Integer> resultList = new ArrayList<>();
            for (JsonNode jsonNode : nodeList) {
                int index = -1;
                int curr = toStartIndex(this.start, jsonNode.size());
                int stop = toEndIndex(this.stop, jsonNode.size());
                if (jsonNode.isArray()) {
                    var arrNode = (ArrayNode) jsonNode;
                    for (; curr < stop; curr++) {
                        if (arrNode.get(curr).equals(valueNode)) {
                            index = curr;
                            break;
                        }
                    }
                    resultList.add(index);
                    if (isLegacy) {
                        return resultList;
                    }
                } else {
                    if (isLegacy) {
                        throw new CacheException("-WRONGTYPE wrong type of path value - expected array but found "
                                + jsonNode.getNodeType().name().toLowerCase());
                    }
                    resultList.add(null);
                }
            }
            if (resultList.isEmpty() && isLegacy) {
                throw new CacheException("Path '" + RespUtil.ascii(jsonPath) + "' does not exist");
            }
            return resultList;
        } catch (JsonProcessingException e) {
            throw new CacheException(e);
        }

    }

    private int toStartIndex(int start, int size) {
        return start < 0 ? Math.max(size + start, 0) : Math.min(start, size);
    }

    private int toEndIndex(int stop, int size) {
        // 0 means to the end of the array
        return stop <= 0 ? Math.max(size + stop, 0) : Math.min(stop, size);
    }
}
