package org.infinispan.notifications.cachelistener;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletionStage;

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
   CompletionStage<Void> notifyCacheEntryCreated(K key, V value, Metadata metadata, boolean pre, InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a {@link org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent}
    * event.
    */
   CompletionStage<Void> notifyCacheEntryModified(K key, V value, Metadata metadata, V previousValue, Metadata previousMetadata, boolean pre,
                                 InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a {@link org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent}
    * event.
    */
   CompletionStage<Void> notifyCacheEntryRemoved(K key, V previousValue, Metadata previousMetadata, boolean pre, InvocationContext ctx,
                                FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a {@link org.infinispan.notifications.cachelistener.event.CacheEntryVisitedEvent}
    * event.
    */
   CompletionStage<Void> notifyCacheEntryVisited(K key, V value, boolean pre,
                                InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a {@link org.infinispan.notifications.cachelistener.event.CacheEntriesEvictedEvent}
    * event.
    */
   // DataContainer eviction is sync
   CompletionStage<Void> notifyCacheEntriesEvicted(Collection<Map.Entry<K, V>> entries,
                                  InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a CacheEntryExpired event.
    */
   CompletionStage<Void> notifyCacheEntryExpired(K key, V value, Metadata metadata, InvocationContext ctx);

   /**
    * Notifies all registered listeners of a {@link org.infinispan.notifications.cachelistener.event.CacheEntryInvalidatedEvent}
    * event.
    */
   CompletionStage<Void> notifyCacheEntryInvalidated(K key, V value, Metadata metadata, boolean pre,
                                    InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a {@link org.infinispan.notifications.cachelistener.event.CacheEntryLoadedEvent}
    * event.
    */
   CompletionStage<Void> notifyCacheEntryLoaded(K key, V value, boolean pre,
                               InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a {@link org.infinispan.notifications.cachelistener.event.CacheEntryActivatedEvent}
    * event.
    */
   CompletionStage<Void> notifyCacheEntryActivated(K key, V value, boolean pre,
                                  InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a {@link org.infinispan.notifications.cachelistener.event.CacheEntryPassivatedEvent}
    * event.
    */
   // Callers of passivation are sync
   CompletionStage<Void> notifyCacheEntryPassivated(K key, V value, boolean pre,
                                   InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a transaction completion event.
    *
    * @param transaction the transaction that has just completed
    * @param successful  if true, the transaction committed.  If false, this is a rollback event
    */
   CompletionStage<Void> notifyTransactionCompleted(GlobalTransaction transaction, boolean successful, InvocationContext ctx);

   /**
    * Notifies all registered listeners of a transaction registration event.
    *
    * @param globalTransaction
    */
   // Sync local transaction registered
   CompletionStage<Void> notifyTransactionRegistered(GlobalTransaction globalTransaction, boolean isOriginLocal);

   // Callers sync - until additional parts of topology updates - not in user thread
   CompletionStage<Void> notifyDataRehashed(ConsistentHash oldCH, ConsistentHash newCH, ConsistentHash unionCH, int newTopologyId, boolean pre);

   // Callers sync - until additional parts of topology updates - not in user thread
   CompletionStage<Void> notifyTopologyChanged(CacheTopology oldTopology, CacheTopology newTopology, int newTopologyId, boolean pre);

   // Callers sync - until additional parts of topology updates - not in user thread
   CompletionStage<Void> notifyPartitionStatusChanged(AvailabilityMode mode, boolean pre);

   // Callers sync - done in periodic persistence thread - not in user thread
   CompletionStage<Void> notifyPersistenceAvailabilityChanged(boolean available);

   /**
    * Returns whether there is at least one listener regitstered for the given annotation
    * @param annotationClass annotation to test for
    * @return true if there is a listener mapped to the annotation, otherwise false
    */
   boolean hasListener(Class<? extends Annotation> annotationClass);
}
