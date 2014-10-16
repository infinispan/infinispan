package org.infinispan.iteration.impl;

import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterable;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.transaction.impl.LocalTransaction;

/**
 * CloseableIterable implementation that will enhance another CloseableIterable to use the provided context values in the
 * iteration process properly.  That is context values will take precendence over values found from the iterator.
 *
 * @author wburns
 * @since 7.0
 */
public class TransactionAwareCloseableIterable<K, C> implements CloseableIterable<CacheEntry<K, C>> {
   protected final CloseableIterable<CacheEntry<K, C>>  iterable;
   protected final TxInvocationContext<LocalTransaction> ctx;
   protected final Cache<K, ?> cache;

   public TransactionAwareCloseableIterable(CloseableIterable<CacheEntry<K, C>> iterable,
                                            TxInvocationContext<LocalTransaction> ctx, Cache<K, ?> cache) {
      this.iterable = iterable;
      this.ctx = ctx;
      this.cache = cache;
   }

   @Override
   public void close() {
      iterable.close();
   }

   @Override
   public CloseableIterator<CacheEntry<K, C>> iterator() {
      return new TransactionAwareCloseableIterator(iterable.iterator(), ctx, cache);
   }
}
