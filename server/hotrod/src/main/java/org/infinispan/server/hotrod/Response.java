package org.infinispan.server.hotrod;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.infinispan.CacheSet;
import org.infinispan.commons.util.Util;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.hotrod.iteration.IterableIterationResult;

/**
 * A basic responses. The rest of this file contains other response types.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
public abstract class Response {
   protected final byte version;
   protected final long messageId;
   protected final String cacheName;
   protected final short clientIntel;
   protected final HotRodOperation operation;
   protected final OperationStatus status;
   protected final int topologyId;

   protected Response(byte version, long messageId, String cacheName, short clientIntel, HotRodOperation operation, OperationStatus status, int topologyId) {
      this.version = version;
      this.messageId = messageId;
      this.cacheName = cacheName;
      this.clientIntel = clientIntel;
      this.operation = operation;
      this.status = status;
      this.topologyId = topologyId;
   }

   public byte getVersion() {
      return version;
   }

   public long getMessageId() {
      return messageId;
   }

   public String getCacheName() {
      return cacheName;
   }

   public short getClientIntel() {
      return clientIntel;
   }

   public HotRodOperation getOperation() {
      return operation;
   }

   public OperationStatus getStatus() {
      return status;
   }

   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public String toString() {
      return "Response{" +
            "version=" + version +
            ", messageId=" + messageId +
            ", cacheName='" + cacheName + '\'' +
            ", clientIntel=" + clientIntel +
            ", operation=" + operation +
            ", status=" + status +
            ", topologyId=" + topologyId +
            '}';
   }
}

class ResponseWithPrevious extends Response {
   protected final Optional<byte[]> previous;

   ResponseWithPrevious(byte version, long messageId, String cacheName, short clientIntel, HotRodOperation operation,
                        OperationStatus status, int topologyId, Optional<byte[]> previous) {
      super(version, messageId, cacheName, clientIntel, operation, status, topologyId);
      this.previous = previous;
   }

   @Override
   public String toString() {
      return "ResponseWithPrevious{" +
            "version=" + version +
            ", messageId=" + messageId +
            ", cacheName='" + cacheName + '\'' +
            ", clientIntel=" + clientIntel +
            ", operation=" + operation +
            ", status=" + status +
            ", topologyId=" + topologyId +
            ", previous=" + Util.printArray(previous.orElse(null), true) +
            '}';
   }
}

class GetResponse extends Response {
   protected final byte[] data;

   GetResponse(byte version, long messageId, String cacheName, short clientIntel, HotRodOperation operation,
               OperationStatus status, int topologyId, byte[] data) {
      super(version, messageId, cacheName, clientIntel, operation, status, topologyId);
      this.data = data;
   }

   @Override
   public String toString() {
      return "GetResponse{" +
            "version=" + version +
            ", messageId=" + messageId +
            ", cacheName='" + cacheName + '\'' +
            ", clientIntel=" + clientIntel +
            ", operation=" + operation +
            ", status=" + status +
            ", topologyId=" + topologyId +
            ", data=" + Util.printArray(data, true) +
            '}';
   }
}

class BulkGetResponse extends Response {
   protected final int count;
   protected final CacheSet<Map.Entry<byte[], byte[]>> entries;

   BulkGetResponse(byte version, long messageId, String cacheName, short clientIntel, int topologyId, int count,
                   CacheSet<Map.Entry<byte[], byte[]>> entries) {
      super(version, messageId, cacheName, clientIntel, HotRodOperation.BULK_GET, OperationStatus.Success, topologyId);
      this.count = count;
      this.entries = entries;
   }

   @Override
   public String toString() {
      return "BulkGetResponse{" +
            "version=" + version +
            ", messageId=" + messageId +
            ", cacheName='" + cacheName + '\'' +
            ", clientIntel=" + clientIntel +
            ", operation=" + operation +
            ", status=" + status +
            ", topologyId=" + topologyId +
            ", count=" + count +
            ", entries=" + entries +
            '}';
   }
}

class BulkGetKeysResponse extends Response {
   protected final int scope;
   protected final Iterator<byte[]> iterator;

   BulkGetKeysResponse(byte version, long messageId, String cacheName, short clientIntel, int topologyId, int scope,
                       Iterator<byte[]> iterator) {
      super(version, messageId, cacheName, clientIntel, HotRodOperation.BULK_GET_KEYS, OperationStatus.Success,
            topologyId);
      this.scope = scope;
      this.iterator = iterator;
   }

   @Override
   public String toString() {
      return "BulkGetKeysResponse{" +
            "version=" + version +
            ", messageId=" + messageId +
            ", cacheName='" + cacheName + '\'' +
            ", clientIntel=" + clientIntel +
            ", operation=" + operation +
            ", status=" + status +
            ", topologyId=" + topologyId +
            ", scope=" + scope +
            '}';
   }
}

class GetAllResponse extends Response {
   protected final Map<byte[], byte[]> entries;

   GetAllResponse(byte version, long messageId, String cacheName, short clientIntel, int topologyId,
                  Map<byte[], byte[]> entries) {
      super(version, messageId, cacheName, clientIntel, HotRodOperation.GET_ALL, OperationStatus.Success, topologyId);
      this.entries = entries;
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder().append("GetAllResponse").append("{")
            .append("version=").append(version)
            .append(", messageId=").append(messageId)
            .append(", operation=").append(operation)
            .append(", status=").append(status)
            .append(", entries=[");
      entries.forEach((k, v) -> {
         sb.append(Util.printArray(k, true));
         sb.append('=');
         sb.append(Util.printArray(v, true));
      });
      return sb.append("]}").toString();
   }
}

class IterationStartResponse extends Response {
   protected final String iterationId;

   IterationStartResponse(byte version, long messageId, String cacheName, short clientIntel, int topologyId,
                          String iterationId) {
      super(version, messageId, cacheName, clientIntel, HotRodOperation.ITERATION_START, OperationStatus.Success, topologyId);
      this.iterationId = iterationId;
   }

   @Override
   public String toString() {
      return "IterationStartResponse{" +
            "version=" + version +
            ", messageId=" + messageId +
            ", cacheName='" + cacheName + '\'' +
            ", clientIntel=" + clientIntel +
            ", operation=" + operation +
            ", status=" + status +
            ", topologyId=" + topologyId +
            ", iterationId=" + iterationId +
            '}';
   }
}

class IterationNextResponse extends Response {
   protected final IterableIterationResult iterationResult;

   IterationNextResponse(byte version, long messageId, String cacheName, short clientIntel, int topologyId,
                         IterableIterationResult iterationResult) {
      super(version, messageId, cacheName, clientIntel, HotRodOperation.ITERATION_NEXT,
            iterationResult.getStatusCode(), topologyId);
      this.iterationResult = iterationResult;
   }

   @Override
   public String toString() {
      return "IterationNextResponse{" +
            "version=" + version +
            ", messageId=" + messageId +
            ", cacheName='" + cacheName + '\'' +
            ", clientIntel=" + clientIntel +
            ", operation=" + operation +
            ", status=" + status +
            ", topologyId=" + topologyId +
            '}';
   }
}

class EmptyResponse extends Response {
   protected EmptyResponse(byte version, long messageId, String cacheName, short clientIntel, HotRodOperation operation, OperationStatus status, int topologyId) {
      super(version, messageId, cacheName, clientIntel, operation, status, topologyId);
   }

   @Override
   public String toString() {
      return "EmptyResponse{" +
            "version=" + version +
            ", messageId=" + messageId +
            ", cacheName='" + cacheName + '\'' +
            ", clientIntel=" + clientIntel +
            ", operation=" + operation +
            ", status=" + status +
            ", topologyId=" + topologyId +
            '}';
   }
}

class GetWithVersionResponse extends GetResponse {
   protected final long dataVersion;

   GetWithVersionResponse(byte version, long messageId, String cacheName, short clientIntel, HotRodOperation operation,
                          OperationStatus status, int topologyId, byte[] data, long dataVersion) {
      super(version, messageId, cacheName, clientIntel, operation, status, topologyId, data);
      this.dataVersion = dataVersion;
   }

   @Override
   public String toString() {
      return "GetWithVersionResponse{" +
            "version=" + version +
            ", messageId=" + messageId +
            ", cacheName='" + cacheName + '\'' +
            ", clientIntel=" + clientIntel +
            ", operation=" + operation +
            ", status=" + status +
            ", topologyId=" + topologyId +
            ", data=" + Util.printArray(data, true) +
            ", dataVersion=" + dataVersion +
            '}';
   }
}

class GetWithMetadataResponse extends GetResponse {
   protected final long dataVersion;
   protected final long created;
   protected final int lifespan;
   protected final long lastUsed;
   protected final int maxIdle;

   GetWithMetadataResponse(byte version, long messageId, String cacheName, short clientIntel, HotRodOperation operation,
                           OperationStatus status, int topologyId, byte[] data, long dataVersion, long created, int lifespan,
                           long lastUsed, int maxIdle) {
      super(version, messageId, cacheName, clientIntel, operation, status, topologyId, data);
      this.dataVersion = dataVersion;
      this.created = created;
      this.lifespan = lifespan;
      this.lastUsed = lastUsed;
      this.maxIdle = maxIdle;
   }

   GetWithMetadataResponse(byte version, long messageId, String cacheName, short clientIntel, HotRodOperation operation,
                           OperationStatus status, int topologyId) {
      this(version, messageId, cacheName, clientIntel, operation, status, topologyId, null, -1, -1, -1, -1, -1);
   }

   @Override
   public String toString() {
      return "GetWithMetadataResponse{" +
            "version=" + version +
            ", messageId=" + messageId +
            ", cacheName='" + cacheName + '\'' +
            ", clientIntel=" + clientIntel +
            ", operation=" + operation +
            ", status=" + status +
            ", topologyId=" + topologyId +
            ", data=" + Util.printArray(data, true) +
            ", dataVersion=" + dataVersion +
            ", created=" + created +
            ", lifespan=" + lifespan +
            ", lastUsed=" + lastUsed +
            ", maxIdle=" + maxIdle +
            '}';
   }
}

class GetStreamResponse extends GetWithMetadataResponse {
   protected final int offset;

   GetStreamResponse(byte version, long messageId, String cacheName, short clientIntel, HotRodOperation operation,
                     OperationStatus status, int topologyId, byte[] data, int offset, long dataVersion, long created, int lifespan,
                     long lastUsed, int maxIdle) {
      super(version, messageId, cacheName, clientIntel, operation, status, topologyId, data, dataVersion, created, lifespan, lastUsed, maxIdle);
      this.offset = offset;
   }

   GetStreamResponse(byte version, long messageId, String cacheName, short clientIntel, HotRodOperation operation, OperationStatus status, int topologyId) {
      super(version, messageId, cacheName, clientIntel, operation, status, topologyId);
      offset = 0;
   }

   @Override
   public String toString() {
      return "GetStreamResponse{" +
            "version=" + version +
            ", messageId=" + messageId +
            ", cacheName='" + cacheName + '\'' +
            ", clientIntel=" + clientIntel +
            ", operation=" + operation +
            ", status=" + status +
            ", topologyId=" + topologyId +
            ", data=" + Util.printArray(data, true) +
            ", offset=" + offset +
            ", dataVersion=" + dataVersion +
            ", created=" + created +
            ", lifespan=" + lifespan +
            ", lastUsed=" + lastUsed +
            ", maxIdle=" + maxIdle +
            '}';
   }
}

class StatsResponse extends Response {
   final Map<String, String> stats;

   StatsResponse(byte version, long messageId, String cacheName, short clientIntel, Map<String, String> stats,
                 int topologyId) {
      super(version, messageId, cacheName, clientIntel, HotRodOperation.STATS, OperationStatus.Success, topologyId);
      this.stats = stats;
   }

   @Override
   public String toString() {
      return "StatsResponse{" +
            "version=" + version +
            ", messageId=" + messageId +
            ", cacheName='" + cacheName + '\'' +
            ", clientIntel=" + clientIntel +
            ", operation=" + operation +
            ", status=" + status +
            ", topologyId=" + topologyId +
            ", stats=" + stats +
            '}';
   }
}

class QueryResponse extends Response {
   final byte[] result;

   QueryResponse(byte version, long messageId, String cacheName, short clientIntel, int topologyId, byte[] result) {
      super(version, messageId, cacheName, clientIntel, HotRodOperation.QUERY, OperationStatus.Success, topologyId);
      this.result = result;
   }

   @Override
   public String toString() {
      return "QueryResponse{" +
            "version=" + version +
            ", messageId=" + messageId +
            ", cacheName='" + cacheName + '\'' +
            ", clientIntel=" + clientIntel +
            ", operation=" + operation +
            ", status=" + status +
            ", topologyId=" + topologyId +
            ", result=" + Util.printArray(result, true) +
            '}';
   }
}

class AuthMechListResponse extends Response {
   final Set<String> mechs;

   AuthMechListResponse(byte version, long messageId, String cacheName, short clientIntel, Set<String> mechs,
                        int topologyId) {
      super(version, messageId, cacheName, clientIntel, HotRodOperation.AUTH_MECH_LIST, OperationStatus.Success,
            topologyId);
      this.mechs = mechs;
   }

   @Override
   public String toString() {
      return "AuthMechListResponse{" +
            "version=" + version +
            ", messageId=" + messageId +
            ", cacheName='" + cacheName + '\'' +
            ", clientIntel=" + clientIntel +
            ", operation=" + operation +
            ", status=" + status +
            ", topologyId=" + topologyId +
            ", mechs=" + mechs +
            '}';
   }
}

class AuthResponse extends Response {
   final byte[] challenge;

   AuthResponse(byte version, long messageId, String cacheName, short clientIntel, byte[] challenge,
                int topologyId) {
      super(version, messageId, cacheName, clientIntel, HotRodOperation.AUTH, OperationStatus.Success,
            topologyId);
      this.challenge = challenge;
   }

   @Override
   public String toString() {
      return "AuthResponse{" +
            "version=" + version +
            ", messageId=" + messageId +
            ", cacheName='" + cacheName + '\'' +
            ", clientIntel=" + clientIntel +
            ", operation=" + operation +
            ", status=" + status +
            ", topologyId=" + topologyId +
            ", challenge=" + Util.printArray(challenge, true) +
            '}';
   }
}

class SizeResponse extends Response {
   final long size;

   SizeResponse(byte version, long messageId, String cacheName, short clientIntel, int topologyId, long size) {
      super(version, messageId, cacheName, clientIntel, HotRodOperation.SIZE, OperationStatus.Success,
            topologyId);
      this.size = size;
   }

   @Override
   public String toString() {
      return "SizeResponse{" +
            "version=" + version +
            ", messageId=" + messageId +
            ", cacheName='" + cacheName + '\'' +
            ", clientIntel=" + clientIntel +
            ", operation=" + operation +
            ", status=" + status +
            ", topologyId=" + topologyId +
            ", size=" + size +
            '}';
   }
}

class ExecResponse extends Response {
   final byte[] result;

   ExecResponse(byte version, long messageId, String cacheName, short clientIntel, int topologyId, byte[] result) {
      super(version, messageId, cacheName, clientIntel, HotRodOperation.EXEC, OperationStatus.Success,
            topologyId);
      this.result = result;
   }

   @Override
   public String toString() {
      return "ExecResponse{" +
            "version=" + version +
            ", messageId=" + messageId +
            ", cacheName='" + cacheName + '\'' +
            ", clientIntel=" + clientIntel +
            ", operation=" + operation +
            ", status=" + status +
            ", topologyId=" + topologyId +
            ", result=" + Util.printArray(result, true) +
            '}';
   }
}


abstract class AbstractTopologyResponse {
   final int topologyId;
   final Map<Address, ServerAddress> serverEndpointsMap;
   final int numSegments;

   protected AbstractTopologyResponse(int topologyId, Map<Address, ServerAddress> serverEndpointsMap, int numSegments) {
      this.topologyId = topologyId;
      this.serverEndpointsMap = serverEndpointsMap;
      this.numSegments = numSegments;
   }
}

abstract class AbstractHashDistAwareResponse extends AbstractTopologyResponse {

   final int numOwners;
   final byte hashFunction;
   final int hashSpace;

   protected AbstractHashDistAwareResponse(int topologyId, Map<Address, ServerAddress> serverEndpointsMap,
                                           int numSegments, int numOwners, byte hashFunction, int hashSpace) {
      super(topologyId, serverEndpointsMap, numSegments);
      this.numOwners = numOwners;
      this.hashFunction = hashFunction;
      this.hashSpace = hashSpace;
   }
}

class TopologyAwareResponse extends AbstractTopologyResponse {

   protected TopologyAwareResponse(int topologyId, Map<Address, ServerAddress> serverEndpointsMap, int numSegments) {
      super(topologyId, serverEndpointsMap, numSegments);
   }
}

class HashDistAwareResponse extends AbstractHashDistAwareResponse {

   protected HashDistAwareResponse(int topologyId, Map<Address, ServerAddress> serverEndpointsMap, int numSegments, int numOwners, byte hashFunction, int hashSpace) {
      super(topologyId, serverEndpointsMap, numSegments, numOwners, hashFunction, hashSpace);
   }
}

class HashDistAware11Response extends AbstractHashDistAwareResponse {
   final int numVNodes;

   protected HashDistAware11Response(int topologyId, Map<Address, ServerAddress> serverEndpointsMap, int numOwners,
                                     byte hashFunction, int hashSpace, int numVNodes) {
      super(topologyId, serverEndpointsMap, 0, numOwners, hashFunction, hashSpace);
      this.numVNodes = numVNodes;
   }
}

class HashDistAware20Response extends AbstractTopologyResponse {
   final byte hashFunction;

   protected HashDistAware20Response(int topologyId, Map<Address, ServerAddress> serverEndpointsMap, int numSegments,
                                     byte hashFunction) {
      super(topologyId, serverEndpointsMap, numSegments);
      this.hashFunction = hashFunction;
   }
}

class TransactionResponse extends Response {

   final int xaReturnCode;

   TransactionResponse(byte version, long messageId, String cacheName, short clientIntel,
         HotRodOperation operation, OperationStatus status, int topologyId, int xaReturnCode) {
      super(version, messageId, cacheName, clientIntel, operation, status, topologyId);
      this.xaReturnCode = xaReturnCode;
   }
}
