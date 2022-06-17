package org.infinispan.hotrod.impl.multimap;

import java.util.Collection;
import java.util.concurrent.CompletionStage;

import org.infinispan.api.common.CacheEntryCollection;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.commons.marshall.AdaptiveBufferSizePredictor;
import org.infinispan.commons.marshall.BufferSizePredictor;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.hotrod.exceptions.RemoteCacheManagerNotStartedException;
import org.infinispan.hotrod.impl.HotRodTransport;
import org.infinispan.hotrod.impl.cache.RemoteCacheImpl;
import org.infinispan.hotrod.impl.logging.Log;
import org.infinispan.hotrod.impl.logging.LogFactory;
import org.infinispan.hotrod.impl.multimap.operations.ContainsEntryMultimapOperation;
import org.infinispan.hotrod.impl.multimap.operations.ContainsKeyMultimapOperation;
import org.infinispan.hotrod.impl.multimap.operations.ContainsValueMultimapOperation;
import org.infinispan.hotrod.impl.multimap.operations.GetKeyMultimapOperation;
import org.infinispan.hotrod.impl.multimap.operations.GetKeyWithMetadataMultimapOperation;
import org.infinispan.hotrod.impl.multimap.operations.MultimapOperationsFactory;
import org.infinispan.hotrod.impl.multimap.operations.PutKeyValueMultimapOperation;
import org.infinispan.hotrod.impl.multimap.operations.RemoveEntryMultimapOperation;
import org.infinispan.hotrod.impl.multimap.operations.RemoveKeyMultimapOperation;
import org.infinispan.hotrod.marshall.MarshallerUtil;
import org.infinispan.hotrod.multimap.RemoteMultimapCache;

/**
 * Remote implementation of {@link RemoteMultimapCache}
 *
 * @since 14.0
 */
public class RemoteMultimapCacheImpl<K, V> implements RemoteMultimapCache<K, V> {

   private static final Log log = LogFactory.getLog(RemoteMultimapCacheImpl.class, Log.class);

   private final RemoteCacheImpl<K, Collection<V>> cache;
   private final HotRodTransport hotRodTransport;
   private MultimapOperationsFactory operationsFactory;
   private Marshaller marshaller;
   private final BufferSizePredictor keySizePredictor = new AdaptiveBufferSizePredictor();
   private final BufferSizePredictor valueSizePredictor = new AdaptiveBufferSizePredictor();
   private final boolean supportsDuplicates;

   public void init() {
      operationsFactory = new MultimapOperationsFactory(
            hotRodTransport.getChannelFactory(),
            cache.getName(),
            hotRodTransport.getConfiguration(),
            hotRodTransport.getCodec(),
            cache.getDataFormat(),
            cache.getClientStatistics());
      this.marshaller = hotRodTransport.getMarshaller();
   }

   public RemoteMultimapCacheImpl(HotRodTransport hotRodTransport, RemoteCacheImpl<K, Collection<V>> cache) {
      this(hotRodTransport, cache, false);
   }

   public RemoteMultimapCacheImpl(HotRodTransport hotRodTransport, RemoteCacheImpl<K, Collection<V>> cache, boolean supportsDuplicates) {
      if (log.isTraceEnabled()) {
         log.tracef("Creating multimap remote cache: %s", cache.getName());
      }
      this.cache = cache;
      this.hotRodTransport = hotRodTransport;
      this.supportsDuplicates = supportsDuplicates;
   }

   @Override
   public CompletionStage<Void> put(K key, V value, CacheWriteOptions options) {
      if (log.isTraceEnabled()) {
         log.tracef("About to add (K,V): (%s, %s) %s", key, value, options);
      }
      assertRemoteCacheManagerIsStarted();
      K objectKey = isObjectStorage() ? key : null;
      byte[] marshallKey = MarshallerUtil.obj2bytes(marshaller, key, keySizePredictor);
      byte[] marshallValue = MarshallerUtil.obj2bytes(marshaller, value, valueSizePredictor);

      PutKeyValueMultimapOperation<K> op = operationsFactory.newPutKeyValueOperation(objectKey,  marshallKey, marshallValue, CacheWriteOptions.DEFAULT, supportsDuplicates);
      return op.execute();
   }

   @Override
   public CompletionStage<Collection<V>> get(K key, CacheOptions options) {
      if (log.isTraceEnabled()) {
         log.tracef("About to call get (K): (%s)", key);
      }
      assertRemoteCacheManagerIsStarted();
      K objectKey = isObjectStorage() ? key : null;
      byte[] marshallKey = MarshallerUtil.obj2bytes(marshaller, key, keySizePredictor);

      GetKeyMultimapOperation<K, V> gco = operationsFactory.newGetKeyMultimapOperation(objectKey, marshallKey, options, supportsDuplicates);
      return gco.execute();
   }

   @Override
   public CompletionStage<CacheEntryCollection<K, V>> getWithMetadata(K key, CacheOptions options) {
      if (log.isTraceEnabled()) {
         log.tracef("About to call getWithMetadata (K): (%s)", key);
      }
      assertRemoteCacheManagerIsStarted();
      K objectKey = isObjectStorage() ? key : null;
      byte[] marshallKey = MarshallerUtil.obj2bytes(marshaller, key, keySizePredictor);
      GetKeyWithMetadataMultimapOperation<K, V> operation
            = operationsFactory.newGetKeyWithMetadataMultimapOperation(objectKey, marshallKey, options, supportsDuplicates);
      return operation.execute();
   }

   @Override
   public CompletionStage<Boolean> remove(K key, CacheOptions options) {
      if (log.isTraceEnabled()) {
         log.tracef("About to remove (K): (%s)", key);
      }
      assertRemoteCacheManagerIsStarted();
      K objectKey = isObjectStorage() ? key : null;
      byte[] marshallKey = MarshallerUtil.obj2bytes(marshaller, key, keySizePredictor);
      RemoveKeyMultimapOperation<K> removeOperation = operationsFactory.newRemoveKeyOperation(objectKey, marshallKey, options, supportsDuplicates);
      return removeOperation.execute();
   }

   @Override
   public CompletionStage<Boolean> remove(K key, V value, CacheOptions options) {
      if (log.isTraceEnabled()) {
         log.tracef("About to remove (K,V): (%s, %s)", key, value);
      }
      assertRemoteCacheManagerIsStarted();
      K objectKey = isObjectStorage() ? key : null;
      byte[] marshallKey = MarshallerUtil.obj2bytes(marshaller, key, keySizePredictor);
      byte[] marshallValue = MarshallerUtil.obj2bytes(marshaller, value, valueSizePredictor);
      RemoveEntryMultimapOperation<K> removeOperation = operationsFactory.newRemoveEntryOperation(objectKey, marshallKey, marshallValue, options, supportsDuplicates);
      return removeOperation.execute();
   }

   @Override
   public CompletionStage<Boolean> containsKey(K key, CacheOptions options) {
      if (log.isTraceEnabled()) {
         log.tracef("About to call contains (K): (%s)", key);
      }
      assertRemoteCacheManagerIsStarted();
      K objectKey = isObjectStorage() ? key : null;
      byte[] marshallKey = MarshallerUtil.obj2bytes(marshaller, key, keySizePredictor);
      ContainsKeyMultimapOperation<K> containsKeyOperation = operationsFactory.newContainsKeyOperation(objectKey, marshallKey, options, supportsDuplicates);
      return containsKeyOperation.execute();
   }

   @Override
   public CompletionStage<Boolean> containsValue(V value, CacheOptions options) {
      if (log.isTraceEnabled()) {
         log.tracef("About to call contains (V): (%s)", value);
      }
      assertRemoteCacheManagerIsStarted();
      byte[] marshallValue = MarshallerUtil.obj2bytes(marshaller, value, valueSizePredictor);
      ContainsValueMultimapOperation containsValueOperation = operationsFactory.newContainsValueOperation(marshallValue, options, supportsDuplicates);
      return containsValueOperation.execute();
   }

   @Override
   public CompletionStage<Boolean> containsEntry(K key, V value, CacheOptions options) {
      if (log.isTraceEnabled()) {
         log.tracef("About to call contais(K,V): (%s, %s)", key, value);
      }
      assertRemoteCacheManagerIsStarted();
      K objectKey = isObjectStorage() ? key : null;
      byte[] marshallKey = MarshallerUtil.obj2bytes(marshaller, key, keySizePredictor);
      byte[] marshallValue = MarshallerUtil.obj2bytes(marshaller, value, valueSizePredictor);
      ContainsEntryMultimapOperation<K> containsOperation = operationsFactory.newContainsEntryOperation(objectKey, marshallKey, marshallValue, options, supportsDuplicates);
      return containsOperation.execute();
   }

   @Override
   public CompletionStage<Long> size(CacheOptions options) {
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
      if (!hotRodTransport.isStarted()) {
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
