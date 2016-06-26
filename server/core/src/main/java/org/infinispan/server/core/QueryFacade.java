package org.infinispan.server.core;

import org.infinispan.AdvancedCache;

/**
 * Query facade
 *
 * @author Galder Zamarreño
 * @author wburns
 * @since 9.0
 */
public interface QueryFacade {
   byte[] query(AdvancedCache<byte[], byte[]> cache, byte[] query);
}
