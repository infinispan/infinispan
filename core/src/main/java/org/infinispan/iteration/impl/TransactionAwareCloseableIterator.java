package org.infinispan.iteration.impl;

import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.filter.Converter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.transaction.impl.LocalTransaction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Class that provides transactional support so that the iterator will use the values in the context if they exist.
 * This will keep track of seen values from the transactional context and if the transactional context is updated while
 * iterating on this iterator it will see those updates unless the changed value was already seen by the iterator.
 *
 * @author wburns
 * @since 7.0
 */
public class TransactionAwareCloseableIterator<K, V, C> extends RemovableEntryIterator<K, V, C> {
   private final TxInvocationContext<LocalTransaction> ctx;
   // We store all the not yet seen context entries here.  We rely on the fact that the cache entry reference is updated
   // if a change occurs in between iterations to see updates.
   private final List<CacheEntry> contextEntries;
   private final Set<Object> seenContextKeys = new HashSet<>();
   private final KeyValueFilter<? super K, ? super V> filter;
   private final Converter<? super K, ? super V, ? extends C> converter;
   private final boolean filterAndConvert;
   private final InternalEntryFactory entryFactory;

   public TransactionAwareCloseableIterator(CloseableIterator<CacheEntry<K, V>> realIterator,
         KeyValueFilter<? super K, ? super V> filter,
         Converter<? super K, ? super V, ? extends C> converter, 
         TxInvocationContext<LocalTransaction> ctx, Cache<K, V> cache) {
      super(realIterator, cache, false);
      this.ctx = ctx;
      this.filter = filter;
      if (filter instanceof KeyValueFilterConverter && (filter == converter || converter == null)) {
         // perform filtering and conversion in a single step
         filterAndConvert = true;
         this.converter = null;
      } else {
         filterAndConvert = false;
         this.converter = converter;
      }
      this.entryFactory = cache.getAdvancedCache().getComponentRegistry().getComponent(InternalEntryFactory.class);
      contextEntries = new ArrayList<>(ctx.getLookedUpEntries().values());
      currentValue = getNextFromIterator();
   }

   protected CacheEntry<K, V> filterEntry(CacheEntry<K, V> entry) {
      if (filterAndConvert) {
         K key = entry.getKey();
         C converted = ((KeyValueFilterConverter<K, V, C>)filter).filterAndConvert(
               key, entry.getValue(), entry.getMetadata());
         if (converted != null) {
            entry = entryFactory.create(entry);
            entry.setValue((V) converted);
            return entry;
         }
      } else if (filter == null || 
            filter.accept(entry.getKey(), entry.getValue(), entry.getMetadata())) {
         return entry;
      }
      return null;
   }

   @Override
   protected CacheEntry<K, C> getNextFromIterator() {
      CacheEntry<K, V> returnedEntry = null;
      // We first have to exhaust all of our context entries
      CacheEntry<K, V> entry;
      while (returnedEntry == null && !contextEntries.isEmpty() && 
            (entry = contextEntries.remove(0)) != null) {
         seenContextKeys.add(entry.getKey());
         if (!ctx.isEntryRemovedInContext(entry.getKey()) && !entry.isNull()) {
            returnedEntry = filterEntry(entry);
         }
      }
      if (returnedEntry == null) {
         while (realIterator.hasNext()) {
            CacheEntry<K, V> iteratedEntry = realIterator.next();
            CacheEntry contextEntry;
            // If the value was in the context then we ignore the stored value since we use the context value
            if ((contextEntry = ctx.lookupEntry(iteratedEntry.getKey())) != null) {
               if (seenContextKeys.add(contextEntry.getKey()) && !contextEntry.isRemoved() && !contextEntry.isNull() &&
                     (returnedEntry = filterEntry(contextEntry)) != null) {
                  break;
               }
               
            } else {
               // Filter and conversion were already done by the iterator
               return (CacheEntry<K, C>) iteratedEntry;
            }
         }
      }

      if (returnedEntry == null) {
         // We do a last check to make sure no additional values were added to our context while iterating
         for (CacheEntry lookedUpEntry : ctx.getLookedUpEntries().values()) {
            if (seenContextKeys.add(lookedUpEntry.getKey()) && !lookedUpEntry.isRemoved() && !lookedUpEntry.isNull()) {
               if (returnedEntry == null) {
                  returnedEntry = lookedUpEntry;
               } else {
                  contextEntries.add(lookedUpEntry);
               }
            }
         }
      }
      if (returnedEntry != null && converter != null) {
         C newValue = converter.convert(returnedEntry.getKey(), returnedEntry.getValue(),
               returnedEntry.getMetadata());
         returnedEntry = entryFactory.create(returnedEntry);
         returnedEntry.setValue((V) newValue);
      }
      return (CacheEntry<K, C>) returnedEntry;
   }
}
