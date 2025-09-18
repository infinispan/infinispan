package org.infinispan.client.hotrod.impl.multimap.operations;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.client.hotrod.multimap.MetadataCollection;
import org.infinispan.commons.marshall.BufferSizePredictor;
import org.infinispan.commons.marshall.Marshaller;

import com.google.errorprone.annotations.Immutable;

/**
 * Factory for {@link HotRodOperation} objects on Multimap.
 *
 * @author karesti@redhat.com
 * @since 9.2
 */
@Immutable
public class DefaultMultimapOperationsFactory implements MultimapOperationsFactory {

   private final InternalRemoteCache<?, ?> remoteCache;
   private final Marshaller marshaller;
   private final BufferSizePredictor keySizePredictor;
   private final BufferSizePredictor valueSizePredictor;

   public DefaultMultimapOperationsFactory(InternalRemoteCache<?, ?> remoteCache, Marshaller marshaller,
                                           BufferSizePredictor keySizePredictor, BufferSizePredictor valueSizePredictor) {
      this.remoteCache = remoteCache;
      this.marshaller = marshaller;
      this.keySizePredictor = keySizePredictor;
      this.valueSizePredictor = valueSizePredictor;
   }

   @Override
   public <K, V> HotRodOperation<Collection<V>> newGetKeyMultimapOperation(K key, boolean supportsDuplicates) {
      return new GetKeyMultimapOperation<>(remoteCache, MarshallerUtil.obj2bytes(marshaller, key, keySizePredictor),
            supportsDuplicates);
   }

   @Override
   public <K, V> HotRodOperation<MetadataCollection<V>> newGetKeyWithMetadataMultimapOperation(K key, boolean supportsDuplicates) {
      return new GetKeyWithMetadataMultimapOperation<>(remoteCache, MarshallerUtil.obj2bytes(marshaller, key, keySizePredictor),
            supportsDuplicates);
   }

   @Override
   public <K, V> HotRodOperation<Void> newPutKeyValueOperation(K key, V value, long lifespan,
                                                               TimeUnit lifespanTimeUnit, long maxIdle,
                                                               TimeUnit maxIdleTimeUnit, boolean supportsDuplicates) {
      return new PutKeyValueMultimapOperation(remoteCache, MarshallerUtil.obj2bytes(marshaller, key, keySizePredictor),
            MarshallerUtil.obj2bytes(marshaller, value, valueSizePredictor), lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, supportsDuplicates);
   }

   @Override
   public <K> HotRodOperation<Boolean> newRemoveKeyOperation(K key, boolean supportsDuplicates) {
      return new RemoveKeyMultimapOperation(remoteCache, MarshallerUtil.obj2bytes(marshaller, key, keySizePredictor), supportsDuplicates);
   }

   @Override
   public <K, V> HotRodOperation<Boolean> newRemoveEntryOperation(K key, V value, boolean supportsDuplicates) {
      return new RemoveEntryMultimapOperation(remoteCache, MarshallerUtil.obj2bytes(marshaller, key, keySizePredictor),
            MarshallerUtil.obj2bytes(marshaller, value, valueSizePredictor), supportsDuplicates);
   }

   @Override
   public <K, V> HotRodOperation<Boolean> newContainsEntryOperation(K key, V value, boolean supportsDuplicates) {
      return new ContainsEntryMultimapOperation(remoteCache, MarshallerUtil.obj2bytes(marshaller, key, keySizePredictor),
            MarshallerUtil.obj2bytes(marshaller, value, valueSizePredictor), supportsDuplicates);
   }

   @Override
   public <K> HotRodOperation<Boolean> newContainsKeyOperation(K key, boolean supportsDuplicates) {
      return new ContainsKeyMultimapOperation(remoteCache, MarshallerUtil.obj2bytes(marshaller, key, keySizePredictor), supportsDuplicates);
   }

   @Override
   public HotRodOperation<Boolean> newContainsValueOperation(byte[] value, boolean supportsDuplicates) {
      return new ContainsValueMultimapOperation(remoteCache, value, -1, TimeUnit.MILLISECONDS, -1, TimeUnit.MILLISECONDS, supportsDuplicates);
   }

   @Override
   public HotRodOperation<Long> newSizeOperation(boolean supportsDuplicates) {
      return new SizeMultimapOperation(remoteCache, supportsDuplicates);
   }
}
