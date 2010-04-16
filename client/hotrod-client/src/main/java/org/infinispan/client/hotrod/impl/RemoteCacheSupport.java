package org.infinispan.client.hotrod.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.CacheSupport;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.config.Configuration;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.CacheManager;
import org.infinispan.util.concurrent.NotifyingFuture;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public abstract class RemoteCacheSupport<K,V> extends CacheSupport<K,V> implements RemoteCache<K,V> {


   @Override
   public NotifyingFuture<Boolean> replaceAsync(K key, V newValue, long version) {
      return replaceAsync(key, newValue, version, 0);
   }

   @Override
   public NotifyingFuture<Boolean> replaceAsync(K key, V newValue, long version, int lifespanSeconds) {
      return replaceAsync(key, newValue, version, 0, 0);
   }

   @Override
   public boolean replace(K key, V newValue, long version) {
      return replace(key, newValue, version, 0);
   }

   @Override
   public CacheManager getCacheManager() {
      throw new UnsupportedOperationException("Use getRemoteCacheManager() instead.");
   }

   @Override
   public boolean replace(K key, V newValue, long version, int lifespanSeconds) {
      return replace(key, newValue, version, lifespanSeconds, 0);
   }

   @Override
   public void putForExternalRead(K key, V value) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void addListener(Object listener) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void removeListener(Object listener) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Set<Object> getListeners() {
      throw new UnsupportedOperationException();
   }

   @Override
   public int size() {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean isEmpty() {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean containsValue(Object value) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Set<K> keySet() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Collection<V> values() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Set<Entry<K, V>> entrySet() {
      throw new UnsupportedOperationException();
   }

   @Override
   public void evict(K key) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Configuration getConfiguration() {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean startBatch() {
      throw new UnsupportedOperationException();
   }

   @Override
   public void endBatch(boolean successful) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean remove(Object key, Object value) {
      throw new UnsupportedOperationException();
   }

   @Override
   public NotifyingFuture<Boolean> removeAsync(Object key, Object value) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      throw new UnsupportedOperationException();
   }


   @Override
   public NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      throw new UnsupportedOperationException();
   }

   @Override
   public AdvancedCache<K, V> getAdvancedCache() {
      throw new UnsupportedOperationException();
   }

   @Override
   public void compact() {
      throw new UnsupportedOperationException();
   }

   @Override
   public ComponentStatus getStatus() {
      throw new UnsupportedOperationException();
   }
}
