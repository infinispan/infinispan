package org.infinispan.remoting.responses;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A response that signals the named cache is not running on the target node.
 *
 * @author Dan Berindei
 * @since 6.0
 */
@ProtoTypeId(ProtoStreamTypeIds.CACHE_NOT_FOUND_RESPONSE)
public class CacheNotFoundResponse extends InvalidResponse {
   public static final CacheNotFoundResponse INSTANCE = new CacheNotFoundResponse();

   private CacheNotFoundResponse() {
   }

   @ProtoFactory
   static CacheNotFoundResponse getInstance() {
      return INSTANCE;
   }
}
