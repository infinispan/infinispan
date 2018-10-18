package org.infinispan.notifications.cachelistener;

import java.util.Collection;
import java.util.Map;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.ClassLoaderAwareListenable;
import org.infinispan.notifications.DataConversionAwareListenable;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * Public interface with all allowed notifications.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public interface CacheNotifier<K, V> extends DataConversionAwareListenable<K, V>, ClassLoaderAwareListenable {

   /**
    * Notifies all registered listeners of a {@link org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent}
    * event.
    */
   void notifyCacheEntryCreated(K key, V value, Metadata metadata, boolean pre, InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a {@link org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent}
    * event.
    */
   void notifyCacheEntryModified(K key, V value, Metadata metadata, V previousValue, Metadata previousMetadata, boolean pre,
                                 InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a {@link org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent}
    * event.
    */
   void notifyCacheEntryRemoved(K key, V previousValue, Metadata previousMetadata, boolean pre, InvocationContext ctx,
                                FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a {@link org.infinispan.notifications.cachelistener.event.CacheEntryVisitedEvent}
    * event.
    */
   void notifyCacheEntryVisited(K key, V value, boolean pre,
                                InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a {@link org.infinispan.notifications.cachelistener.event.CacheEntriesEvictedEvent}
    * event.
    */
   void notifyCacheEntriesEvicted(Collection<Map.Entry<K, V>> entries, InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a CacheEntryExpired event.
    */
   void notifyCacheEntryExpired(K key, V value, Metadata metadata, InvocationContext ctx);

   /**
    * Notifies all registered listeners of a {@link org.infinispan.notifications.cachelistener.event.CacheEntryInvalidatedEvent}
    * event.
    */
   void notifyCacheEntryInvalidated(K key, V value, Metadata metadata, boolean pre,
                                    InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a {@link org.infinispan.notifications.cachelistener.event.CacheEntryLoadedEvent}
    * event.
    */
   void notifyCacheEntryLoaded(K key, V value, boolean pre,
                               InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a {@link org.infinispan.notifications.cachelistener.event.CacheEntryActivatedEvent}
    * event.
    */
   void notifyCacheEntryActivated(K key, V value, boolean pre,
                                  InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a {@link org.infinispan.notifications.cachelistener.event.CacheEntryPassivatedEvent}
    * event.
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

   void notifyPersistenceAvailabilityChanged(boolean available);
}
