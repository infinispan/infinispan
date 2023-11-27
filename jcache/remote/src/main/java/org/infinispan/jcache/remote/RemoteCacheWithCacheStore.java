package org.infinispan.jcache.remote;

import javax.cache.configuration.MutableConfiguration;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.jcache.Exceptions;

public abstract class RemoteCacheWithCacheStore<K, V> extends RemoteCacheWrapper<K, V> {
   private final CacheLoader<K, V> jcacheLoader;
   private final CacheWriter<? super K, ? super V> jcacheWriter;
   private final MutableConfiguration<K, V> configuration;

   public RemoteCacheWithCacheStore(RemoteCache<K, V> delegate,
         CacheLoader<K, V> jcacheLoader,
         CacheWriter<? super K, ? super V> jcacheWriter,
         MutableConfiguration<K, V> configuration) {
      super(delegate);
      this.jcacheLoader = jcacheLoader;
      this.jcacheWriter = jcacheWriter;
      this.configuration = configuration;
   }

   @SuppressWarnings("unchecked")
   @Override
   public V get(Object key) {
      V value = super.get(key);
      if (value == null) {
         try {
            value = loadFromCacheLoader((K) key);
         } catch (ClassCastException ex) {
            //Don't load.
         }
      }
      return value;
   }

   private V loadFromCacheLoader(K key) {
      if (jcacheLoader == null || !configuration.isReadThrough()) {
         return null;
      }
      V value = null;
      try {
         value = jcacheLoader.load(key);
      } catch (Exception ex) {
         throw Exceptions.launderCacheLoaderException(ex);
      }
      if (value != null) {
         onLoad(key, value);
      }
      return value;
   }

   protected abstract void onLoad(K key, V value);
}
