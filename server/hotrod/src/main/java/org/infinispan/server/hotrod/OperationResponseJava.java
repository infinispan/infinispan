package org.infinispan.server.hotrod;

/**
 * Static helper method for bridging a {@link HotRodOperation} to a {@link OperationResponse} enumeration.
 *
 * @author wburns
 * @since 9.0
 */
class OperationResponseJava {
   private OperationResponseJava() { }

   static Object operationToResponse(HotRodOperation op) {
      switch (op) {
         case PutRequest: return OperationResponse.PutResponse();
         case GetRequest: return OperationResponse.GetResponse();
         case PutIfAbsentRequest: return OperationResponse.PutIfAbsentResponse();
         case ReplaceRequest: return OperationResponse.ReplaceResponse();
         case ReplaceIfUnmodifiedRequest: return OperationResponse.ReplaceIfUnmodifiedResponse();
         case RemoveRequest: return OperationResponse.RemoveResponse();
         case RemoveIfUnmodifiedRequest: return OperationResponse.RemoveIfUnmodifiedResponse();
         case ContainsKeyRequest: return OperationResponse.ContainsKeyResponse();
         case GetWithVersionRequest: return OperationResponse.GetWithVersionResponse();
         case ClearRequest: return OperationResponse.ClearResponse();
         case StatsRequest: return OperationResponse.StatsResponse();
         case PingRequest: return OperationResponse.PingResponse();
         case BulkGetRequest: return OperationResponse.BulkGetResponse();
         case GetWithMetadataRequest: return OperationResponse.GetWithMetadataResponse();
         case BulkGetKeysRequest: return OperationResponse.BulkGetKeysResponse();
         case QueryRequest: return OperationResponse.QueryResponse();
         case AuthMechListRequest: return OperationResponse.AuthMechListResponse();
         case AuthRequest: return OperationResponse.AuthResponse();
         case AddClientListenerRequest: return OperationResponse.AddClientListenerResponse();
         case RemoveClientListenerRequest: return OperationResponse.RemoveClientListenerResponse();
         case ExecRequest: return OperationResponse.ExecResponse();
         case PutAllRequest: return OperationResponse.PutAllResponse();
         case GetAllRequest: return OperationResponse.GetAllResponse();
         default: throw new IllegalArgumentException("Unsupported operation: " + op);
      }
   }
}
