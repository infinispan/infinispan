package org.infinispan.notifications.cachelistener;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.notifications.ClassLoaderAwareFilteringListenable;
import org.infinispan.notifications.ClassLoaderAwareListenable;
import org.infinispan.notifications.FilteringListenable;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.Collection;

/**
 * Public interface with all allowed notifications.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public interface CacheNotifier extends ClassLoaderAwareFilteringListenable, ClassLoaderAwareListenable {

   /**
    * Notifies all registered listeners of a CacheEntryCreated event.
    */
   void notifyCacheEntryCreated(Object key, Object value, boolean pre,
         InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a CacheEntryModified event.
    */
   void notifyCacheEntryModified(Object key, Object value,
         boolean created, boolean pre, InvocationContext ctx,
         FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a CacheEntryRemoved event.
    */
   void notifyCacheEntryRemoved(Object key, Object value, Object oldValue,
         boolean pre, InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a CacheEntryVisited event.
    */
   void notifyCacheEntryVisited(Object key, Object value, boolean pre,
         InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a CacheEntriesEvicted event.
    */
   void notifyCacheEntriesEvicted(Collection<InternalCacheEntry> entries,
         InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Syntactic sugar
    * @param key key evicted
    * @param value value evicted
    * @param ctx context
    */
   void notifyCacheEntryEvicted(Object key, Object value,
         InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a CacheEntryInvalidated event.
    */
   void notifyCacheEntryInvalidated(Object key, Object value, boolean pre,
         InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a CacheEntryLoaded event.
    */
   void notifyCacheEntryLoaded(Object key, Object value, boolean pre,
         InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a CacheEntryActivated event.
    */
   void notifyCacheEntryActivated(Object key, Object value, boolean pre,
         InvocationContext ctx, FlagAffectedCommand command);

   /**
    * Notifies all registered listeners of a CacheEntryPassivated event.
    */
   void notifyCacheEntryPassivated(Object key, Object value, boolean pre,
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
   void notifyTransactionRegistered(GlobalTransaction globalTransaction, InvocationContext ctx);

   void notifyDataRehashed(ConsistentHash oldCH, ConsistentHash newCH, int newTopologyId, boolean pre);

   void notifyTopologyChanged(ConsistentHash oldConsistentHash, ConsistentHash newConsistentHash, int newTopologyId, boolean pre);

}