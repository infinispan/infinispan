package org.infinispan.server.hotrod.tx.table;

import org.infinispan.util.ByteString;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
public interface CacheNameCollector {

   void expectedSize(int size);

   void addCache(ByteString cacheName, Status status);

   void noTransactionFound();

}
