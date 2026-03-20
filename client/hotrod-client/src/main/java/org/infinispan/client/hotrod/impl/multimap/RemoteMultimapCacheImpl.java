package org.infinispan.client.hotrod.impl.multimap;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.exceptions.RemoteCacheManagerNotStartedException;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.multimap.operations.DefaultMultimapOperationsFactory;
import org.infinispan.client.hotrod.impl.multimap.operations.MultimapOperationsFactory;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;
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

   private static final Log log = LogFactory.getLog(RemoteMultimapCacheImpl.class);

   private final InternalRemoteCache<K, Collection<V>> cache;
   private final RemoteCacheManager remoteCacheManager;
   private MultimapOperationsFactory operationsFactory;
   private OperationDispatcher dispatcher;
   private Marshaller defaultMarshaller;
   private final BufferSizePredictor keySizePredictor = new AdaptiveBufferSizePredictor();
   private final BufferSizePredictor valueSizePredictor = new AdaptiveBufferSizePredictor();
   private final boolean supportsDuplicates;

   public void init() {
      operationsFactory = new DefaultMultimapOperationsFactory(cache, remoteCacheManager.getMarshaller(),
            new AdaptiveBufferSizePredictor(), new AdaptiveBufferSizePredictor());
      dispatcher = cache.getDispatcher();
      this.defaultMarshaller = remoteCacheManager.getMarshaller();
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
         log.tracef("About to add (K,V): (%s, %s) lifespan:%d, maxIdle:%d", key, value, 0, 0);
      }
      assertRemoteCacheManagerIsStarted();

      HotRodOperation<Void> op = operationsFactory.newPutKeyValueOperation(key, value,
             0, MILLISECONDS, 0, MILLISECONDS, supportsDuplicates);
      return dispatcher.execute(op).toCompletableFuture();
   }

   @Override
   public CompletableFuture<Collection<V>> get(K key) {
      if (log.isTraceEnabled()) {
         log.tracef("About to call get (K): (%s)", key);
      }
      assertRemoteCacheManagerIsStarted();

      HotRodOperation<Collection<V>> gco = operationsFactory.newGetKeyMultimapOperation(key, supportsDuplicates);
      return dispatcher.execute(gco).toCompletableFuture();
   }

   @Override
   public CompletableFuture<MetadataCollection<V>> getWithMetadata(K key) {
      if (log.isTraceEnabled()) {
         log.tracef("About to call getWithMetadata (K): (%s)", key);
      }
      assertRemoteCacheManagerIsStarted();
      HotRodOperation<MetadataCollection<V>> operation
            = operationsFactory.newGetKeyWithMetadataMultimapOperation(key, supportsDuplicates);
      return dispatcher.execute(operation).toCompletableFuture();
   }

   @Override
   public CompletableFuture<Boolean> remove(K key) {
      if (log.isTraceEnabled()) {
         log.tracef("About to remove (K): (%s)", key);
      }
      assertRemoteCacheManagerIsStarted();
      HotRodOperation<Boolean> removeOperation = operationsFactory.newRemoveKeyOperation(key, supportsDuplicates);
      return dispatcher.execute(removeOperation).toCompletableFuture();
   }

   @Override
   public CompletableFuture<Boolean> remove(K key, V value) {
      if (log.isTraceEnabled()) {
         log.tracef("About to remove (K,V): (%s, %s)", key, value);
      }
      assertRemoteCacheManagerIsStarted();
      HotRodOperation<Boolean> removeOperation = operationsFactory.newRemoveEntryOperation(key, value, supportsDuplicates);
      return dispatcher.execute(removeOperation).toCompletableFuture();
   }

   @Override
   public CompletableFuture<Boolean> containsKey(K key) {
      if (log.isTraceEnabled()) {
         log.tracef("About to call contains (K): (%s)", key);
      }
      assertRemoteCacheManagerIsStarted();
      HotRodOperation<Boolean> containsKeyOperation = operationsFactory.newContainsKeyOperation(key, supportsDuplicates);
      return dispatcher.execute(containsKeyOperation).toCompletableFuture();
   }

   @Override
   public CompletableFuture<Boolean> containsValue(V value) {
      if (log.isTraceEnabled()) {
         log.tracef("About to call contains (V): (%s)", value);
      }
      assertRemoteCacheManagerIsStarted();
      byte[] marshallValue = MarshallerUtil.obj2bytes(defaultMarshaller, value, valueSizePredictor);
      HotRodOperation<Boolean> containsValueOperation = operationsFactory.newContainsValueOperation(marshallValue, supportsDuplicates);
      return dispatcher.execute(containsValueOperation).toCompletableFuture();
   }

   @Override
   public CompletableFuture<Boolean> containsEntry(K key, V value) {
      if (log.isTraceEnabled()) {
         log.tracef("About to call contais(K,V): (%s, %s)", key, value);
      }
      assertRemoteCacheManagerIsStarted();
      HotRodOperation<Boolean> containsOperation = operationsFactory.newContainsEntryOperation(key, value, supportsDuplicates);
      return dispatcher.execute(containsOperation).toCompletableFuture();
   }

   @Override
   public CompletableFuture<Long> size() {
      if (log.isTraceEnabled()) {
         log.trace("About to call size");
      }
      assertRemoteCacheManagerIsStarted();
      return dispatcher.execute(operationsFactory.newSizeOperation(supportsDuplicates))
            .toCompletableFuture();
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
}
