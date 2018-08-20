package org.infinispan.client.hotrod.impl.multimap;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.exceptions.RemoteCacheManagerNotStartedException;
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
import org.infinispan.commons.marshall.Marshaller;

/**
 * Remote implementation of {@link RemoteMultimapCache}
 *
 * @author karesti@redhat.com
 * @since 9.2
 */
public class RemoteMultimapCacheImpl<K, V> implements RemoteMultimapCache<K, V> {

   private static final Log log = LogFactory.getLog(RemoteMultimapCacheImpl.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private RemoteCacheImpl<K, Collection<V>> cache;
   private final RemoteCacheManager remoteCacheManager;
   private MultimapOperationsFactory operationsFactory;
   private Marshaller marshaller;
   private int estimateKeySize;
   private int estimateValueSize;
   private long defaultLifespan = 0;
   private long defaultMaxIdleTime = 0;

   public void init() {
      operationsFactory = new MultimapOperationsFactory(remoteCacheManager.getChannelFactory(),
            cache.getName(),
            remoteCacheManager.getCodec(),
            remoteCacheManager.getConfiguration(), cache.getDataFormat());
      this.marshaller = remoteCacheManager.getMarshaller();
      this.estimateKeySize = remoteCacheManager.getConfiguration().keySizeEstimate();
      this.estimateValueSize = remoteCacheManager.getConfiguration().valueSizeEstimate();
   }

   public RemoteMultimapCacheImpl(RemoteCacheManager rcm, RemoteCache<K, Collection<V>> cache) {
      if (trace) {
         log.tracef("Creating multimap remote cache: %s", cache.getName());
      }
      this.cache = (RemoteCacheImpl<K, Collection<V>>) cache;
      this.remoteCacheManager = rcm;
   }

   @Override
   public CompletableFuture<Void> put(K key, V value) {
      if (trace) {
         log.tracef("About to add (K,V): (%s, %s) lifespan:%d, maxIdle:%d", key, value, defaultLifespan, defaultMaxIdleTime);
      }
      assertRemoteCacheManagerIsStarted();
      K objectKey = isObjectStorage() ? key : null;
      byte[] marshallKey = MarshallerUtil.obj2bytes(marshaller, key, true, estimateKeySize, estimateValueSize);
      byte[] marshallValue = MarshallerUtil.obj2bytes(marshaller, value, false, estimateKeySize, estimateValueSize);

      PutKeyValueMultimapOperation op = operationsFactory.newPutKeyValueOperation(objectKey,
            marshallKey, marshallValue, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
      return op.execute();
   }

   @Override
   public CompletableFuture<Collection<V>> get(K key) {
      if (trace) {
         log.tracef("About to call get (K): (%s)", key);
      }
      assertRemoteCacheManagerIsStarted();
      K objectKey = isObjectStorage() ? key : null;
      byte[] marshallKey = MarshallerUtil.obj2bytes(marshaller, key, true, estimateKeySize, estimateValueSize);

      GetKeyMultimapOperation<V> gco = operationsFactory.newGetKeyMultimapOperation(objectKey, marshallKey);
      return gco.execute();
   }

   @Override
   public CompletableFuture<MetadataCollection<V>> getWithMetadata(K key) {
      if (trace) {
         log.tracef("About to call getWithMetadata (K): (%s)", key);
      }
      assertRemoteCacheManagerIsStarted();
      K objectKey = isObjectStorage() ? key : null;
      byte[] marshallKey = MarshallerUtil.obj2bytes(marshaller, key, true, estimateKeySize, estimateValueSize);
      GetKeyWithMetadataMultimapOperation<V> operation
            = operationsFactory.newGetKeyWithMetadataMultimapOperation(objectKey, marshallKey);
      return operation.execute();
   }

   @Override
   public CompletableFuture<Boolean> remove(K key) {
      if (trace) {
         log.tracef("About to remove (K): (%s)", key);
      }
      assertRemoteCacheManagerIsStarted();
      K objectKey = isObjectStorage() ? key : null;
      byte[] marshallKey = MarshallerUtil.obj2bytes(marshaller, key, true, estimateKeySize, estimateValueSize);
      RemoveKeyMultimapOperation removeOperation = operationsFactory.newRemoveKeyOperation(objectKey, marshallKey);
      return removeOperation.execute();
   }

   @Override
   public CompletableFuture<Boolean> remove(K key, V value) {
      if (trace) {
         log.tracef("About to remove (K,V): (%s, %s)", key, value);
      }
      assertRemoteCacheManagerIsStarted();
      K objectKey = isObjectStorage() ? key : null;
      byte[] marshallKey = MarshallerUtil.obj2bytes(marshaller, key, true, estimateKeySize, estimateValueSize);
      byte[] marshallValue = MarshallerUtil.obj2bytes(marshaller, value, false, estimateKeySize, estimateValueSize);
      RemoveEntryMultimapOperation removeOperation = operationsFactory.newRemoveEntryOperation(objectKey, marshallKey, marshallValue);
      return removeOperation.execute();
   }

   @Override
   public CompletableFuture<Boolean> containsKey(K key) {
      if (trace) {
         log.tracef("About to call contains (K): (%s)", key);
      }
      assertRemoteCacheManagerIsStarted();
      K objectKey = isObjectStorage() ? key : null;
      byte[] marshallKey = MarshallerUtil.obj2bytes(marshaller, key, true, estimateKeySize, estimateValueSize);
      ContainsKeyMultimapOperation containsKeyOperation = operationsFactory.newContainsKeyOperation(objectKey, marshallKey);
      return containsKeyOperation.execute();
   }

   @Override
   public CompletableFuture<Boolean> containsValue(V value) {
      if (trace) {
         log.tracef("About to call contains (V): (%s)", value);
      }
      assertRemoteCacheManagerIsStarted();
      byte[] marshallValue = MarshallerUtil.obj2bytes(marshaller, value, false, estimateKeySize, estimateValueSize);
      ContainsValueMultimapOperation containsValueOperation = operationsFactory.newContainsValueOperation(marshallValue);
      return containsValueOperation.execute();
   }

   @Override
   public CompletableFuture<Boolean> containsEntry(K key, V value) {
      if (trace) {
         log.tracef("About to call contais(K,V): (%s, %s)", key, value);
      }
      assertRemoteCacheManagerIsStarted();
      K objectKey = isObjectStorage() ? key : null;
      byte[] marshallKey = MarshallerUtil.obj2bytes(marshaller, key, true, estimateKeySize, estimateValueSize);
      byte[] marshallValue = MarshallerUtil.obj2bytes(marshaller, value, false, estimateKeySize, estimateValueSize);
      ContainsEntryMultimapOperation containsOperation = operationsFactory.newContainsEntryOperation(objectKey, marshallKey, marshallValue);
      return containsOperation.execute();
   }

   @Override
   public CompletableFuture<Long> size() {
      if (trace) {
         log.trace("About to call size");
      }
      assertRemoteCacheManagerIsStarted();
      return operationsFactory.newSizeOperation().execute();
   }

   @Override
   public boolean supportsDuplicates() {
      return false;
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
