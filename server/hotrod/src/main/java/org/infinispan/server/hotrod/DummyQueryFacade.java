package org.infinispan.server.hotrod;

import org.infinispan.AdvancedCache;
import org.infinispan.server.core.QueryFacade;

/**
 * @author Dan Berindei
 * @since 9.0
 */
public class DummyQueryFacade implements QueryFacade {
   @Override
   public byte[] query(AdvancedCache<byte[], byte[]> cache, byte[] query) {
      return query;
   }
}
