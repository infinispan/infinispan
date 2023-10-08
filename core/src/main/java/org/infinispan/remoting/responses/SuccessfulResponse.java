package org.infinispan.remoting.responses;

import java.util.Collection;
import java.util.Map;

import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.MortalCacheValue;
import org.infinispan.container.entries.TransientCacheValue;
import org.infinispan.container.entries.TransientMortalCacheValue;
import org.infinispan.container.entries.metadata.MetadataImmortalCacheValue;
import org.infinispan.container.entries.metadata.MetadataMortalCacheValue;
import org.infinispan.container.entries.metadata.MetadataTransientCacheValue;
import org.infinispan.container.entries.metadata.MetadataTransientMortalCacheValue;

/**
 * A successful response
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface SuccessfulResponse<T> extends ValidResponse<T> {

   SuccessfulResponse SUCCESSFUL_EMPTY_RESPONSE = new SuccessfulObjResponse<>(null);

   static SuccessfulResponse<?> create(Object rv) {
      if (rv == null)
         return SUCCESSFUL_EMPTY_RESPONSE;

      if (rv instanceof Long l)
         return new SuccessfulLongResponse(l);

      if (rv instanceof Boolean b)
         return new SuccessfulBooleanResponse(b);

      if (rv instanceof InternalCacheValue<?>) {
         if (rv instanceof MetadataImmortalCacheValue v)
            return new SuccessfulMetadataImmortalMortalCacheValueResponse(v);

         if (rv instanceof MetadataTransientMortalCacheValue v)
            return new SuccessfulMetadataTransientMortalCacheValueResponse(v);

         if (rv instanceof MetadataMortalCacheValue v)
            return new SuccessfulMetadataMortalCacheValueResponse(v);

         if (rv instanceof MetadataTransientCacheValue v)
            return new SuccessfulMetadataTransientCacheValueResponse(v);

         if (rv instanceof TransientMortalCacheValue v)
            return new SuccessfulTransientMortalCacheValueResponse(v);

         if (rv instanceof MortalCacheValue v)
            return new SuccessfulMortalCacheValueResponse(v);

         if (rv instanceof TransientCacheValue v)
            return new SuccessfulTransientCacheValueResponse(v);

         if (rv instanceof ImmortalCacheValue v)
            return new SuccessfulImmortalCacheValueResponse(v);
      }

      if (rv instanceof Collection<?> collection && !(rv instanceof IntSet))
         return new SuccessfulCollectionResponse<>(collection);

      if (rv.getClass().isArray()) {
         if (rv instanceof byte[] bytes) {
            return new SuccessfulBytesResponse(bytes);
         }
         // suppress unchecked
         return new SuccessfulArrayResponse<>((Object[]) rv);
      }
      if (rv instanceof Map<?, ?> map)
         return new SuccessfulMapResponse<>(map);

      return new SuccessfulObjResponse<>(rv);
   }
}
