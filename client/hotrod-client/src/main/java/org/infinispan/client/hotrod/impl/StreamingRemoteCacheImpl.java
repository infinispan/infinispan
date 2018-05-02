package org.infinispan.client.hotrod.impl;

import static org.infinispan.client.hotrod.impl.Util.await;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.StreamingRemoteCache;
import org.infinispan.client.hotrod.VersionedMetadata;
import org.infinispan.client.hotrod.impl.operations.GetStreamOperation;
import org.infinispan.client.hotrod.impl.operations.PutStreamOperation;

/**
 * Implementation of {@link StreamingRemoteCache}
 *
 * @author Tristan Tarrant
 * @since 9.0
 */

public class StreamingRemoteCacheImpl<K> implements StreamingRemoteCache<K> {
   private final RemoteCacheImpl<K, ?> cache;

   public StreamingRemoteCacheImpl(RemoteCacheImpl<K, ?> cache) {
      this.cache = cache;
   }

   @Override
   public <T extends InputStream & VersionedMetadata> T get(K key) {
      GetStreamOperation op = cache.operationsFactory.newGetStreamOperation(cache.compatKeyIfNeeded(key), cache.keyToBytes(key), 0);
      return (T) await(op.execute());
   }

   @Override
   public OutputStream put(K key) {
      return put(key, -1, TimeUnit.SECONDS, -1, TimeUnit.SECONDS);
   }

   @Override
   public OutputStream put(K key, long lifespan, TimeUnit unit) {
      return put(key, lifespan, unit, -1, TimeUnit.SECONDS);
   }

   @Override
   public OutputStream put(K key, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      PutStreamOperation op = cache.operationsFactory.newPutStreamOperation(cache.compatKeyIfNeeded(key), cache.keyToBytes(key), lifespan, lifespanUnit, maxIdle, maxIdleUnit);
      return await(op.execute());
   }

   @Override
   public OutputStream putIfAbsent(K key) {
      return putIfAbsent(key, -1, TimeUnit.SECONDS, -1, TimeUnit.SECONDS);
   }

   @Override
   public OutputStream putIfAbsent(K key, long lifespan, TimeUnit unit) {
      return putIfAbsent(key, lifespan, unit, -1, TimeUnit.SECONDS);
   }

   @Override
   public OutputStream putIfAbsent(K key, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      PutStreamOperation op = cache.operationsFactory.newPutIfAbsentStreamOperation(cache.compatKeyIfNeeded(key), cache.keyToBytes(key), lifespan, lifespanUnit, maxIdle, maxIdleUnit);
      return await(op.execute());
   }

   @Override
   public OutputStream replaceWithVersion(K key, long version) {
      return replaceWithVersion(key, version, -1, TimeUnit.SECONDS, -1, TimeUnit.SECONDS);
   }

   @Override
   public OutputStream replaceWithVersion(K key, long version, long lifespan, TimeUnit unit) {
      return replaceWithVersion(key, version, lifespan, unit, -1, TimeUnit.SECONDS);
   }

   @Override
   public OutputStream replaceWithVersion(K key, long version, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      PutStreamOperation op = cache.operationsFactory.newPutStreamOperation(cache.compatKeyIfNeeded(key), cache.keyToBytes(key), version, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
      return await(op.execute());
   }
}
