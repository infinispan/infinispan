package org.infinispan.notifications.cachelistener;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.ClassLoaderAwareFilteringListenable;
import org.infinispan.notifications.ClassLoaderAwareListenable;
import org.infinispan.notifications.FilteringListenable;
import org.infinispan.partionhandling.AvailabilityMode;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.Collection;

/**
 * Public interface with all allowed notifications.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public interface CacheNotifier<K, V> extends ClassLoaderAwareFilteringListenable<K, V>, ClassLoaderAwareListenable {

   /**
    * Notifies all registered listeners of a CacheEntryCreated event.
    */
   void notifyCacheEntryCreated(K key, V value, boolean pre, InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a CacheEntryModified event.
    */
   void notifyCacheEntryModified(K key, V value, V previousValue, Metadata previousMetadata, boolean pre,
                                 InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a CacheEntryRemoved event.
    */
   void notifyCacheEntryRemoved(K key, V previousValue, Metadata previousMetadata, boolean pre, InvocationContext ctx,
                                FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a CacheEntryVisited event.
    */
   void notifyCacheEntryVisited(K key, V value, boolean pre,
         InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a CacheEntriesEvicted event.
    */
   void notifyCacheEntriesEvicted(Collection<InternalCacheEntry<? extends K, ? extends V>> entries,
         InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Syntactic sugar
    * @param key key evicted
    * @param value value evicted
    * @param ctx context
    */
   void notifyCacheEntryEvicted(K key, V value,
         InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a CacheEntryInvalidated event.
    */
   void notifyCacheEntryInvalidated(K key, V value, boolean pre,
         InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a CacheEntryLoaded event.
    */
   void notifyCacheEntryLoaded(K key, V value, boolean pre,
         InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a CacheEntryActivated event.
    */
   void notifyCacheEntryActivated(K key, V value, boolean pre,
         InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a CacheEntryPassivated event.
    */
   void notifyCacheEntryPassivated(K key, V value, boolean pre,
         InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a transaction completion event.
    *
    * @param transaction the transaction that has just completed
    * @param successful  if true, the transaction committed.  If false, this is a rollback event
    */
   void notifyTransactionCompleted(GlobalTransaction transaction, boolean successful, InvocationContext ctx);

   /**
    * Notifies all registered listeners of a transaction registration event.
    *
    * @param globalTransaction
    */
   void notifyTransactionRegistered(GlobalTransaction globalTransaction, boolean isOriginLocal);

   void notifyDataRehashed(ConsistentHash oldCH, ConsistentHash newCH, ConsistentHash unionCH, int newTopologyId, boolean pre);

   void notifyTopologyChanged(CacheTopology oldTopology, CacheTopology newTopology, int newTopologyId, boolean pre);

   void notifyPartitionStatusChanged(AvailabilityMode mode, boolean pre);
}
