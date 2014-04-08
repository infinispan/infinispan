package org.infinispan.remoting.responses;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * A response that signals the named cache is not running on the target node.
 *
 * @author Dan Berindei
 * @since 6.0
 */
public class CacheNotFoundResponse extends InvalidResponse {
   public static final CacheNotFoundResponse INSTANCE = new CacheNotFoundResponse();

   private CacheNotFoundResponse() {
   }

   public static class Externalizer extends AbstractExternalizer<CacheNotFoundResponse> {
      @Override
      public void writeObject(ObjectOutput output, CacheNotFoundResponse response) throws IOException {
      }

      @Override
      public CacheNotFoundResponse readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return INSTANCE;
      }

      @Override
      public Integer getId() {
         return Ids.CACHE_NOT_FOUND_RESPONSE;
      }

      @Override
      public Set<Class<? extends CacheNotFoundResponse>> getTypeClasses() {
         return Util.<Class<? extends CacheNotFoundResponse>>asSet(CacheNotFoundResponse.class);
      }
   }
}
