package org.infinispan.client.hotrod.impl;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.StreamingRemoteCache;
import org.infinispan.client.hotrod.VersionedMetadata;
import org.infinispan.client.hotrod.impl.operations.CacheOperationsFactory;
import org.infinispan.client.hotrod.impl.operations.GetStreamStartResponse;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.operations.PutStreamResponse;
import org.infinispan.client.hotrod.impl.operations.PutStreamStartOperation;
import org.infinispan.client.hotrod.impl.protocol.GetInputStream;
import org.infinispan.client.hotrod.impl.protocol.PutOutputStream;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelRecord;
import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;
import org.infinispan.commons.util.concurrent.CompletableFutures;

/**
 * Implementation of {@link StreamingRemoteCache}
 *
 * @author Tristan Tarrant
 * @since 9.0
 */

public class StreamingRemoteCacheImpl<K> implements StreamingRemoteCache<K> {
   private final CacheOperationsFactory factory;
   private final OperationDispatcher dispatcher;
   private final ClientStatistics clientStatistics;

   public StreamingRemoteCacheImpl(InternalRemoteCache<K, ?> cache) {
      this.factory = cache.getOperationsFactory();
      this.dispatcher = cache.getDispatcher();

      if (cache.getRemoteCacheContainer().getConfiguration().statistics().enabled()) {
         clientStatistics = cache.clientStatistics();
      } else {
         clientStatistics = null;
      }
   }

   @Override
   public <T extends InputStream & VersionedMetadata> T get(K key) {
      long startTime = clientStatistics != null ? clientStatistics.time() : 0;
      // TODO: what should we make the batch size be?
      HotRodOperation<GetStreamStartResponse> hro = factory.newGetStreamStartOperation(key, 1 << 13);
      GetStreamStartResponse gsr = dispatcher.await(dispatcher.execute(hro));

      if (gsr == null) {
         if (clientStatistics != null) {
            clientStatistics.dataRead(false, startTime, 1);
         }
         return null;
      }

      if (gsr.complete() && clientStatistics != null) {
         clientStatistics.dataRead(true, startTime, 1);
      }

      //noinspection unchecked
      return (T) new GetInputStream(
            () -> {
               var gsno = factory.newGetStreamNextOperation(gsr.id(), gsr.channel());
               var stage = dispatcher.executeOnSingleAddress(gsno, ChannelRecord.of(gsr.channel()));
               if (clientStatistics != null) {
                  stage.thenApply(gsnr -> {
                     if (gsnr.complete()) {
                        clientStatistics.dataRead(true, startTime, 1);
                     }
                     return gsnr;
                  });
               }
               return stage;
            }, gsr.metadata(), gsr.value(), gsr.complete(),
            () -> {
               var gseo = factory.newGetStreamEndOperation(gsr.id());
               dispatcher.executeOnSingleAddress(gseo, ChannelRecord.of(gsr.channel()));
            });
   }

   @Override
   public OutputStream put(K key) {
      return put(key, -1, TimeUnit.SECONDS, -1, TimeUnit.SECONDS);
   }

   @Override
   public OutputStream put(K key, long lifespan, TimeUnit unit) {
      return put(key, lifespan, unit, -1, TimeUnit.SECONDS);
   }

   private OutputStream handlePutStreamOp(HotRodOperation<PutStreamResponse> hro) {
      long startTime = clientStatistics != null ? clientStatistics.time() : 0;
      PutStreamResponse psr = dispatcher.await(dispatcher.execute(hro));

      return new PutOutputStream((bb, complete) -> {
         var psno = factory.newPutStreamNextOperation(psr.id(), complete, bb, psr.channel());
         var psnr = dispatcher.executeOnSingleAddress(psno, ChannelRecord.of(psr.channel()));
         if (clientStatistics != null && complete) {
            return psnr.thenAccept(___ -> clientStatistics.dataStore(startTime, 1));
         }
         return psnr.thenApply(CompletableFutures.toNullFunction());
      }, psr.channel().alloc(), dispatcher);
   }

   @Override
   public OutputStream put(K key, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      HotRodOperation<PutStreamResponse> hro = factory.newPutStreamStartOperation(key,
            PutStreamStartOperation.VERSION_PUT, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
      return handlePutStreamOp(hro);
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
      HotRodOperation<PutStreamResponse> hro = factory.newPutStreamStartOperation(key,
            PutStreamStartOperation.VERSION_PUT_IF_ABSENT, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
      return handlePutStreamOp(hro);
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
   public OutputStream replaceWithVersion(K key, long version, long lifespan, TimeUnit lifespanUnit, long maxIdle,
                                          TimeUnit maxIdleUnit) {
      HotRodOperation<PutStreamResponse> hro = factory.newPutStreamStartOperation(key, version, lifespan, lifespanUnit,
            maxIdle, maxIdleUnit);
      return handlePutStreamOp(hro);
   }
}
