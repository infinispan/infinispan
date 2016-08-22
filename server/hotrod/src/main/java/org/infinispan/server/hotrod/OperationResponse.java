package org.infinispan.server.hotrod;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Galder Zamarreño
 * @author wburns
 */
public enum OperationResponse {
   PutResponse(0x02),
   GetResponse(0x04),
   PutIfAbsentResponse(0x06),
   ReplaceResponse(0x08),
   ReplaceIfUnmodifiedResponse(0x0A),
   RemoveResponse(0x0C),
   RemoveIfUnmodifiedResponse(0x0E),
   ContainsKeyResponse(0x10),
   GetWithVersionResponse(0x12),
   ClearResponse(0x14),
   StatsResponse(0x16),
   PingResponse(0x18),
   BulkGetResponse(0x1A),
   ErrorResponse(0x50),

   // 1.2
   GetWithMetadataResponse(0x1C),
   BulkGetKeysResponse(0x1E),

   // 1.3
   QueryResponse(0x20),

   // 2.0
   AuthMechListResponse(0x22),
   AuthResponse(0x24),
   AddClientListenerResponse(0x26),
   RemoveClientListenerResponse(0x28),
   SizeResponse(0x2A),
   CacheEntryCreatedEventResponse(0x60),
   CacheEntryModifiedEventResponse(0x61),
   CacheEntryRemovedEventResponse(0x62),
   CacheEntryExpiredEventResponse(0x63),

   // 2.1
   ExecResponse(0x2C),
   PutAllResponse(0x2E),
   GetAllResponse(0x30),

   // 2.3
   IterationStartResponse(0x32),
   IterationNextResponse(0x34),
   IterationEndResponse(0x36);

   private final static Map<Byte, OperationResponse> intMap = new HashMap<>();

   static {
      for (OperationResponse response : OperationResponse.values()) {
         intMap.put(response.code, response);
      }
   }

   private final byte code;

   OperationResponse(int code) {
      this.code = (byte) code;
   }

   public byte getCode() {
      return code;
   }

   public static OperationResponse fromCode(byte code) {
      return intMap.get(code);
   }

   public static OperationResponse toResponse(HotRodOperation op) {
      // Go to java so switch case will be optimized properly
      switch (op) {
         case PutRequest:
            return OperationResponse.PutResponse;
         case GetRequest:
            return OperationResponse.GetResponse;
         case PutIfAbsentRequest:
            return OperationResponse.PutIfAbsentResponse;
         case ReplaceRequest:
            return OperationResponse.ReplaceResponse;
         case ReplaceIfUnmodifiedRequest:
            return OperationResponse.ReplaceIfUnmodifiedResponse;
         case RemoveRequest:
            return OperationResponse.RemoveResponse;
         case RemoveIfUnmodifiedRequest:
            return OperationResponse.RemoveIfUnmodifiedResponse;
         case ContainsKeyRequest:
            return OperationResponse.ContainsKeyResponse;
         case GetWithVersionRequest:
            return OperationResponse.GetWithVersionResponse;
         case ClearRequest:
            return OperationResponse.ClearResponse;
         case StatsRequest:
            return OperationResponse.StatsResponse;
         case PingRequest:
            return OperationResponse.PingResponse;
         case BulkGetRequest:
            return OperationResponse.BulkGetResponse;
         case GetWithMetadataRequest:
            return OperationResponse.GetWithMetadataResponse;
         case BulkGetKeysRequest:
            return OperationResponse.BulkGetKeysResponse;
         case QueryRequest:
            return OperationResponse.QueryResponse;
         case AuthMechListRequest:
            return OperationResponse.AuthMechListResponse;
         case AuthRequest:
            return OperationResponse.AuthResponse;
         case AddClientListenerRequest:
            return OperationResponse.AddClientListenerResponse;
         case RemoveClientListenerRequest:
            return OperationResponse.RemoveClientListenerResponse;
         case ExecRequest:
            return OperationResponse.ExecResponse;
         case PutAllRequest:
            return OperationResponse.PutAllResponse;
         case GetAllRequest:
            return OperationResponse.GetAllResponse;
         default:
            throw new IllegalArgumentException("Unsupported operation: " + op);
      }
   }

   public static HotRodOperation fromResponse(OperationResponse response) {

      switch (response) {
         case PutResponse:
            return HotRodOperation.PutRequest;
         case GetResponse:
            return HotRodOperation.GetRequest;
         case PutIfAbsentResponse:
            return HotRodOperation.PutIfAbsentRequest;
         case ReplaceResponse:
            return HotRodOperation.ReplaceRequest;
         case ReplaceIfUnmodifiedResponse:
            return HotRodOperation.ReplaceIfUnmodifiedRequest;
         case RemoveResponse:
            return HotRodOperation.RemoveRequest;
         case RemoveIfUnmodifiedResponse:
            return HotRodOperation.RemoveIfUnmodifiedRequest;
         case ContainsKeyResponse:
            return HotRodOperation.ContainsKeyRequest;
         case GetWithVersionResponse:
            return HotRodOperation.GetWithVersionRequest;
         case ClearResponse:
            return HotRodOperation.ClearRequest;
         case StatsResponse:
            return HotRodOperation.StatsRequest;
         case PingResponse:
            return HotRodOperation.PingRequest;
         case BulkGetResponse:
            return HotRodOperation.BulkGetRequest;

         case GetWithMetadataResponse:
            return HotRodOperation.GetWithMetadataRequest;
         case BulkGetKeysResponse:
            return HotRodOperation.BulkGetKeysRequest;

         // 1.3
         case QueryResponse:
            return HotRodOperation.QueryRequest;

         // 2.0
         case AuthMechListResponse:
            return HotRodOperation.AuthMechListRequest;
         case AuthResponse:
            return HotRodOperation.AuthRequest;
         case AddClientListenerResponse:
            return HotRodOperation.AddClientListenerRequest;
         case RemoveClientListenerResponse:
            return HotRodOperation.RemoveClientListenerRequest;
         case SizeResponse:
            return HotRodOperation.SizeRequest;

         // 2.1;
         case ExecResponse:
            return HotRodOperation.ExecRequest;
         case PutAllResponse:
            return HotRodOperation.PutAllRequest;
         case GetAllResponse:
            return HotRodOperation.GetAllRequest;

         // 2.3
         case IterationStartResponse:
            return HotRodOperation.IterationStartRequest;
         case IterationNextResponse:
            return HotRodOperation.IterationNextRequest;
         case IterationEndResponse:
            return HotRodOperation.IterationEndRequest;
         default:
            return null;
      }
   }
}

