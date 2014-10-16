package org.infinispan.iteration.impl;

import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.impl.TxInvocationContext;
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
public class TransactionAwareCloseableIterator<K, C> extends RemovableEntryIterator<K, C> {
   private final TxInvocationContext<LocalTransaction> ctx;
   // We store all the not yet seen context entries here.  We rely on the fact that the cache entry reference is updated
   // if a change occurs in between iterations to see updates.
   private final List<CacheEntry> contextEntries;
   private final Set<Object> seenContextKeys = new HashSet<>();

   public TransactionAwareCloseableIterator(CloseableIterator<CacheEntry<K, C>> realIterator,
                                            TxInvocationContext<LocalTransaction> ctx, Cache<K, ?> cache) {
      super(realIterator, cache, false);
      this.ctx = ctx;
      contextEntries = new ArrayList<>(ctx.getLookedUpEntries().values());
      currentValue = getNextFromIterator();
   }

   @Override
   protected CacheEntry<K, C> getNextFromIterator() {
      CacheEntry<K, C> returnedEntry = null;
      // We first have to exhaust all of our context entries
      CacheEntry<K, C> entry;
      while (!contextEntries.isEmpty() && (entry = contextEntries.remove(0)) != null) {
         seenContextKeys.add(entry.getKey());
         if (!ctx.isEntryRemovedInContext(entry.getKey())) {
            returnedEntry = entry;
         }
      }
      if (returnedEntry == null) {
         while (realIterator.hasNext()) {
            CacheEntry<K, C> iteratedEntry = realIterator.next();
            CacheEntry contextEntry;
            // If the value was in the context then we ignore it since we use the context value
            if ((contextEntry = ctx.lookupEntry(iteratedEntry.getKey())) == null) {
               returnedEntry = iteratedEntry;
               break;
            } else {
               if (seenContextKeys.add(contextEntry.getKey()) && !contextEntry.isRemoved()) {
                  returnedEntry = contextEntry;
                  break;
               }
            }
         }
      }

      if (returnedEntry == null) {
         // We do a last check to make sure no additional values were added to our context while iterating
         for (CacheEntry lookedUpEntry : ctx.getLookedUpEntries().values()) {
            if (!seenContextKeys.contains(lookedUpEntry.getKey())) {
               if (returnedEntry == null) {
                  returnedEntry = lookedUpEntry;
               } else {
                  contextEntries.add(lookedUpEntry);
               }
            }
         }
      }
      return returnedEntry;
   }
}
