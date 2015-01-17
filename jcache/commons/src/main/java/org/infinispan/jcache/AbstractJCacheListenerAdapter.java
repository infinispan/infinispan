package org.infinispan.jcache;

public abstract class AbstractJCacheListenerAdapter<K, V> {
   protected final AbstractJCache<K, V> jcache;
   protected final AbstractJCacheNotifier<K, V> notifier;

   public AbstractJCacheListenerAdapter(AbstractJCache<K, V> jcache, AbstractJCacheNotifier<K, V> notifier) {
      this.jcache = jcache;
      this.notifier = notifier;
   }
}
