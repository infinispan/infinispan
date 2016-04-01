package org.infinispan.server.hotrod;

/**
 * Enumeration defining all of the possible hotrod operations
 *
 * @author wburns
 * @since 9.0
 */
public enum HotRodOperation {
   // Puts
   PutRequest(true, true, DecoderRequirements.VALUE, true),
   PutIfAbsentRequest(true, true, DecoderRequirements.VALUE, true),
   // Replace
   ReplaceRequest(true, true, DecoderRequirements.VALUE, true),
   ReplaceIfUnmodifiedRequest(true, true, DecoderRequirements.VALUE, true),
   // Contains
   ContainsKeyRequest(true, false, DecoderRequirements.KEY, true),
   // Gets
   GetRequest(true, false, DecoderRequirements.KEY, true),
   GetWithVersionRequest(true, false, DecoderRequirements.KEY, true),
   GetWithMetadataRequest(true, false, DecoderRequirements.KEY, true),
   // Removes
   RemoveRequest(true, false, DecoderRequirements.KEY, true),
   RemoveIfUnmodifiedRequest(true, false, DecoderRequirements.PARAMETERS, true),

   // Operation(s) that end after Header is read
   PingRequest(false, false, DecoderRequirements.HEADER, false),
   StatsRequest(false, false, DecoderRequirements.HEADER, true),
   ClearRequest(false, false, DecoderRequirements.HEADER, true),
   SizeRequest(false, false, DecoderRequirements.HEADER, true),
   AuthMechListRequest(false, false, DecoderRequirements.HEADER, false),

   // Operation(s) that end after Custom Header is read
   AuthRequest(false, false, DecoderRequirements.HEADER_CUSTOM, false),
   ExecRequest(false, false, DecoderRequirements.HEADER_CUSTOM, true),

   // Operations that end after a Custom Key is read
   BulkGetRequest(false, false, DecoderRequirements.KEY_CUSTOM, true),
   BulkGetKeysRequest(false, false, DecoderRequirements.KEY_CUSTOM, true),
   QueryRequest(false, false, DecoderRequirements.KEY_CUSTOM, true),
   AddClientListenerRequest(false, false, DecoderRequirements.KEY_CUSTOM, true),
   RemoveClientListenerRequest(false, false, DecoderRequirements.KEY_CUSTOM, true),
   IterationStartRequest(false, false, DecoderRequirements.KEY_CUSTOM, true),
   IterationNextRequest(false, false, DecoderRequirements.KEY_CUSTOM, true),
   IterationEndRequest(false, false, DecoderRequirements.KEY_CUSTOM, true),

   // Operations that end after a Custom Value is read
   PutAllRequest(false, false, DecoderRequirements.VALUE_CUSTOM, true),
   GetAllRequest(false, false, DecoderRequirements.VALUE_CUSTOM, true)
   ;

   private final boolean requiresKey;
   private final boolean requiresValue;
   private final DecoderRequirements decodeRequirements;
   private final boolean requiresAuthentication;

   HotRodOperation(boolean requiresKey, boolean requiresValue, DecoderRequirements decodeRequirements,
           boolean requiresAuthentication) {
      this.requiresKey = requiresKey;
      this.requiresValue = requiresValue;
      this.decodeRequirements = decodeRequirements;
      this.requiresAuthentication = requiresAuthentication;
   }

   DecoderRequirements getDecoderRequirements() {
      return decodeRequirements;
   }

   boolean requiresKey() {
      return requiresKey;
   }

   boolean requireValue() {
      return requiresValue;
   }

   boolean requiresAuthentication() { return requiresAuthentication; }

   boolean canSkipIndexing() {
      switch (this) {
         case PutRequest:
         case RemoveRequest:
         case PutIfAbsentRequest:
         case RemoveIfUnmodifiedRequest:
         case ReplaceRequest:
         case ReplaceIfUnmodifiedRequest:
         case PutAllRequest:
            return true;
         default:
            return false;
      }
   }

   boolean canSkipCacheLoading() {
      switch (this) {
         case PutRequest:
         case GetRequest:
         case GetWithVersionRequest:
         case RemoveRequest:
         case ContainsKeyRequest:
         case BulkGetRequest:
         case GetWithMetadataRequest:
         case BulkGetKeysRequest:
         case PutAllRequest:
            return true;
         default:
            return false;
      }
   }

   boolean isNotConditionalAndCanReturnPrevious() {
      return this == PutRequest;
   }

   boolean canReturnPreviousValue() {
      switch (this) {
         case PutRequest:
         case RemoveRequest:
         case PutIfAbsentRequest:
         case ReplaceRequest:
         case ReplaceIfUnmodifiedRequest:
         case RemoveIfUnmodifiedRequest:
            return true;
         default:
            return false;
      }
   }

   boolean isConditional() {
      switch (this) {
         case PutIfAbsentRequest:
         case ReplaceRequest:
         case ReplaceIfUnmodifiedRequest:
         case RemoveIfUnmodifiedRequest:
            return true;
         default:
            return false;
      }
   }
}

enum DecoderRequirements {
   HEADER,
   HEADER_CUSTOM,
   KEY,
   KEY_CUSTOM,
   PARAMETERS,
   VALUE,
   VALUE_CUSTOM
}
