package org.infinispan.eviction.impl;

import net.jcip.annotations.ThreadSafe;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.ImmutableContext;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.notifications.cachelistener.CacheNotifier;

import java.util.Map;

@ThreadSafe
public class EvictionManagerImpl<K, V> implements EvictionManager<K, V> {
   // components to be injected
   private CacheNotifier<K, V> cacheNotifier;
   private ExpirationManager<K, V> expirationManager;

   @Inject
   public void initialize(CacheNotifier<K, V> cacheNotifier, ExpirationManager<K, V> expirationManager) {
      this.cacheNotifier = cacheNotifier;
   }

   @Deprecated
   @Override
   public void processEviction() {
      // This does nothing, as eviction is always done on the same thread that caused it
      expirationManager.processExpiration();
   }

   @Deprecated
   @Override
   public boolean isEnabled() {
      return expirationManager.isEnabled();
   }

   @Override
   public void onEntryEviction(Map<? extends K, InternalCacheEntry<? extends K, ? extends V>> evicted) {
      // don't reuse the threadlocal context as we don't want to include eviction
      // operations in any ongoing transaction, nor be affected by flags
      // especially see ISPN-1154: it's illegal to acquire locks in a committing transaction
      InvocationContext ctx = ImmutableContext.INSTANCE;
      // This is important because we make no external guarantees on the thread
      // that will execute this code, so it could be the user thread, or could
      // be the eviction thread.
      // However, when a user calls cache.evict(), you do want to carry over the
      // contextual information, hence it makes sense for the notifyCacheEntriesEvicted()
      // call to carry on taking an InvocationContext object.
      cacheNotifier.notifyCacheEntriesEvicted(evicted.values(), ctx, null);
   }
}
