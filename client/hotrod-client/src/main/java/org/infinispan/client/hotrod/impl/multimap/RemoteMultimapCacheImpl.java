package org.infinispan.client.hotrod.impl.multimap;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.exceptions.RemoteCacheManagerNotStartedException;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.multimap.operations.ContainsEntryMultimapOperation;
import org.infinispan.client.hotrod.impl.multimap.operations.ContainsKeyMultimapOperation;
import org.infinispan.client.hotrod.impl.multimap.operations.ContainsValueMultimapOperation;
import org.infinispan.client.hotrod.impl.multimap.operations.GetKeyMultimapOperation;
import org.infinispan.client.hotrod.impl.multimap.operations.GetKeyWithMetadataMultimapOperation;
import org.infinispan.client.hotrod.impl.multimap.operations.MultimapOperationsFactory;
import org.infinispan.client.hotrod.impl.multimap.operations.PutKeyValueMultimapOperation;
import org.infinispan.client.hotrod.impl.multimap.operations.RemoveEntryMultimapOperation;
import org.infinispan.client.hotrod.impl.multimap.operations.RemoveKeyMultimapOperation;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.client.hotrod.multimap.MetadataCollection;
import org.infinispan.client.hotrod.multimap.RemoteMultimapCache;
import org.infinispan.commons.marshall.AdaptiveBufferSizePredictor;
import org.infinispan.commons.marshall.BufferSizePredictor;
import org.infinispan.commons.marshall.Marshaller;

/**
 * Remote implementation of {@link RemoteMultimapCache}
 *
 * @author karesti@redhat.com
 * @since 9.2
 */
public class RemoteMultimapCacheImpl<K, V> implements RemoteMultimapCache<K, V> {

   private static final Log log = LogFactory.getLog(RemoteMultimapCacheImpl.class, Log.class);

   private final InternalRemoteCache<K, Collection<V>> cache;
   private final RemoteCacheManager remoteCacheManager;
   private MultimapOperationsFactory operationsFactory;
   private Marshaller marshaller;
   private final BufferSizePredictor keySizePredictor = new AdaptiveBufferSizePredictor();
   private final BufferSizePredictor valueSizePredictor = new AdaptiveBufferSizePredictor();
   private long defaultLifespan = 0;
   private long defaultMaxIdleTime = 0;
   private final boolean supportsDuplicates;

   public void init() {
      operationsFactory = new MultimapOperationsFactory(remoteCacheManager.getChannelFactory(),
            cache.getName(),
            remoteCacheManager.getConfiguration(),
            cache.getDataFormat(),
            cache.clientStatistics());
      this.marshaller = remoteCacheManager.getMarshaller();
   }

   public RemoteMultimapCacheImpl(RemoteCacheManager rcm, RemoteCache<K, Collection<V>> cache) {
      this(rcm, cache, false);
   }

   public RemoteMultimapCacheImpl(RemoteCacheManager rcm, RemoteCache<K, Collection<V>> cache, boolean supportsDuplicates) {
      if (log.isTraceEnabled()) {
         log.tracef("Creating multimap remote cache: %s", cache.getName());
      }
      this.cache = (RemoteCacheImpl<K, Collection<V>>) cache;
      this.remoteCacheManager = rcm;
      this.supportsDuplicates = supportsDuplicates;
   }

   @Override
   public CompletableFuture<Void> put(K key, V value) {
      if (log.isTraceEnabled()) {
         log.tracef("About to add (K,V): (%s, %s) lifespan:%d, maxIdle:%d", key, value, defaultLifespan, defaultMaxIdleTime);
      }
      assertRemoteCacheManagerIsStarted();
      K objectKey = isObjectStorage() ? key : null;
      byte[] marshallKey = MarshallerUtil.obj2bytes(marshaller, key, keySizePredictor);
      byte[] marshallValue = MarshallerUtil.obj2bytes(marshaller, value, valueSizePredictor);

      PutKeyValueMultimapOperation op = operationsFactory.newPutKeyValueOperation(objectKey,
            marshallKey, marshallValue, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS, supportsDuplicates);
      return op.execute();
   }

   @Override
   public CompletableFuture<Collection<V>> get(K key) {
      if (log.isTraceEnabled()) {
         log.tracef("About to call get (K): (%s)", key);
      }
      assertRemoteCacheManagerIsStarted();
      K objectKey = isObjectStorage() ? key : null;
      byte[] marshallKey = MarshallerUtil.obj2bytes(marshaller, key, keySizePredictor);

      GetKeyMultimapOperation<V> gco = operationsFactory.newGetKeyMultimapOperation(objectKey, marshallKey, supportsDuplicates);
      return gco.execute();
   }

   @Override
   public CompletableFuture<MetadataCollection<V>> getWithMetadata(K key) {
      if (log.isTraceEnabled()) {
         log.tracef("About to call getWithMetadata (K): (%s)", key);
      }
      assertRemoteCacheManagerIsStarted();
      K objectKey = isObjectStorage() ? key : null;
      byte[] marshallKey = MarshallerUtil.obj2bytes(marshaller, key, keySizePredictor);
      GetKeyWithMetadataMultimapOperation<V> operation
            = operationsFactory.newGetKeyWithMetadataMultimapOperation(objectKey, marshallKey, supportsDuplicates);
      return operation.execute();
   }

   @Override
   public CompletableFuture<Boolean> remove(K key) {
      if (log.isTraceEnabled()) {
         log.tracef("About to remove (K): (%s)", key);
      }
      assertRemoteCacheManagerIsStarted();
      K objectKey = isObjectStorage() ? key : null;
      byte[] marshallKey = MarshallerUtil.obj2bytes(marshaller, key, keySizePredictor);
      RemoveKeyMultimapOperation removeOperation = operationsFactory.newRemoveKeyOperation(objectKey, marshallKey, supportsDuplicates);
      return removeOperation.execute();
   }

   @Override
   public CompletableFuture<Boolean> remove(K key, V value) {
      if (log.isTraceEnabled()) {
         log.tracef("About to remove (K,V): (%s, %s)", key, value);
      }
      assertRemoteCacheManagerIsStarted();
      K objectKey = isObjectStorage() ? key : null;
      byte[] marshallKey = MarshallerUtil.obj2bytes(marshaller, key, keySizePredictor);
      byte[] marshallValue = MarshallerUtil.obj2bytes(marshaller, value, valueSizePredictor);
      RemoveEntryMultimapOperation removeOperation = operationsFactory.newRemoveEntryOperation(objectKey, marshallKey, marshallValue, supportsDuplicates);
      return removeOperation.execute();
   }

   @Override
   public CompletableFuture<Boolean> containsKey(K key) {
      if (log.isTraceEnabled()) {
         log.tracef("About to call contains (K): (%s)", key);
      }
      assertRemoteCacheManagerIsStarted();
      K objectKey = isObjectStorage() ? key : null;
      byte[] marshallKey = MarshallerUtil.obj2bytes(marshaller, key, keySizePredictor);
      ContainsKeyMultimapOperation containsKeyOperation = operationsFactory.newContainsKeyOperation(objectKey, marshallKey, supportsDuplicates);
      return containsKeyOperation.execute();
   }

   @Override
   public CompletableFuture<Boolean> containsValue(V value) {
      if (log.isTraceEnabled()) {
         log.tracef("About to call contains (V): (%s)", value);
      }
      assertRemoteCacheManagerIsStarted();
      byte[] marshallValue = MarshallerUtil.obj2bytes(marshaller, value, valueSizePredictor);
      ContainsValueMultimapOperation containsValueOperation = operationsFactory.newContainsValueOperation(marshallValue, supportsDuplicates);
      return containsValueOperation.execute();
   }

   @Override
   public CompletableFuture<Boolean> containsEntry(K key, V value) {
      if (log.isTraceEnabled()) {
         log.tracef("About to call contais(K,V): (%s, %s)", key, value);
      }
      assertRemoteCacheManagerIsStarted();
      K objectKey = isObjectStorage() ? key : null;
      byte[] marshallKey = MarshallerUtil.obj2bytes(marshaller, key, keySizePredictor);
      byte[] marshallValue = MarshallerUtil.obj2bytes(marshaller, value, valueSizePredictor);
      ContainsEntryMultimapOperation containsOperation = operationsFactory.newContainsEntryOperation(objectKey, marshallKey, marshallValue, supportsDuplicates);
      return containsOperation.execute();
   }

   @Override
   public CompletableFuture<Long> size() {
      if (log.isTraceEnabled()) {
         log.trace("About to call size");
      }
      assertRemoteCacheManagerIsStarted();
      return operationsFactory.newSizeOperation(supportsDuplicates).execute();
   }

   @Override
   public boolean supportsDuplicates() {
      return supportsDuplicates;
   }

   private void assertRemoteCacheManagerIsStarted() {
      if (!remoteCacheManager.isStarted()) {
         String message = "Cannot perform operations on a multimap cache associated with an unstarted RemoteMultimapCacheManager.";
         if (log.isInfoEnabled()) {
            log.unstartedRemoteCacheManager();
         }
         throw new RemoteCacheManagerNotStartedException(message);
      }
   }

   private boolean isObjectStorage() {
      return cache.isObjectStorage();
   }
}
