package org.infinispan.jcache.remote;

import org.infinispan.jcache.AbstractJCache;
import org.infinispan.jcache.AbstractJCacheListenerAdapter;
import org.infinispan.jcache.AbstractJCacheNotifier;

public class JCacheNotifier<K, V> extends AbstractJCacheNotifier<K, V> {
   @Override
   protected AbstractJCacheListenerAdapter<K, V> createListenerAdapter(AbstractJCache<K, V> jcache,
         AbstractJCacheNotifier<K, V> notifier) {
      return new JCacheListenerAdapter<K, V>(jcache, notifier);
   }
}
