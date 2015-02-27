package org.infinispan.iteration.impl;

import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterable;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.filter.Converter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.transaction.impl.LocalTransaction;

/**
 * CloseableIterable implementation that will enhance another CloseableIterable to use the provided context values in the
 * iteration process properly.  That is context values will take precendence over values found from the iterator.
 *
 * @author wburns
 * @since 7.0
 */
public class TransactionAwareCloseableIterable<K, V, C> implements CloseableIterable<CacheEntry<K, C>> {
   protected final CloseableIterable<CacheEntry<K, C>>  iterable;
   protected final TxInvocationContext<LocalTransaction> ctx;
   protected final Cache<K, ?> cache;
   protected final KeyValueFilter<? super K, ? super V> filter;
   protected final Converter<? super K, ? super V, ? extends C> converter;

   public TransactionAwareCloseableIterable(CloseableIterable<CacheEntry<K, C>> iterable, 
         KeyValueFilter<? super K, ? super V> filter,
         Converter<? super K, ? super V, ? extends C> converter,
         TxInvocationContext<LocalTransaction> ctx, Cache<K, ?> cache) {
      this.iterable = iterable;
      this.ctx = ctx;
      this.cache = cache;
      this.filter = filter;
      this.converter = converter;
   }

   @Override
   public void close() {
      iterable.close();
   }

   @Override
   public CloseableIterator<CacheEntry<K, C>> iterator() {
      return new TransactionAwareCloseableIterator(iterable.iterator(), filter, converter,
            ctx, cache);
   }
}
