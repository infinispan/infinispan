package org.infinispan.iteration.impl;

import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterable;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.filter.Converter;
import org.infinispan.iteration.EntryIterable;
import org.infinispan.transaction.impl.LocalTransaction;

/**
 * {@inheritDoc}
 *
 * @author wburns
 * @since 7.0
 */
public class TransactionAwareEntryIterable<K, V> extends TransactionAwareCloseableIterable<K, V> implements EntryIterable<K, V> {
   private final EntryIterable<K, V> entryIterable;

   public TransactionAwareEntryIterable(EntryIterable<K, V> entryIterable, TxInvocationContext<LocalTransaction> ctx,
                                        Cache<K, V> cache) {
      super(entryIterable, ctx, cache);
      this.entryIterable = entryIterable;
   }

   @Override
   public <C> CloseableIterable<CacheEntry<K, C>> converter(Converter<? super K, ? super V, ? extends C> converter) {
      return new TransactionAwareCloseableIterable<>(entryIterable.converter(converter), ctx, cache);
   }
}
