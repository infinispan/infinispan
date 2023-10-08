package org.infinispan.reactive.publisher.impl;

import static org.testng.AssertJUnit.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.commands.GetAllCommandStressTest;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;
import org.infinispan.test.fwk.InCacheMode;
import org.reactivestreams.Publisher;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.core.Flowable;

@Test(groups = "stress", testName = "PublisherManagerGetKeyStressTest", timeOut = 15 * 60 * 1000)
@InCacheMode(CacheMode.DIST_SYNC)
public class PublisherManagerGetKeyStressTest extends GetAllCommandStressTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      builderUsed = new ConfigurationBuilder();
      builderUsed.clustering().cacheMode(cacheMode).remoteTimeout(30000).stateTransfer().chunkSize(25000);
      createClusteredCaches(CACHE_COUNT, CACHE_NAME, PublisherManagerGetKeyStressTestSCI.INSTANCE, builderUsed);
   }

   @Override
   protected void workerLogic(Cache<Integer, Integer> cache, Set<Integer> threadKeys, int iteration) {
      ClusterPublisherManager<Integer, Integer> cpm = ComponentRegistry.componentOf(cache, ClusterPublisherManager.class);
      CompletionStage<Map<Integer, Integer>> stage = cpm.entryReduction(false, null, threadKeys, null, EnumUtil.EMPTY_BIT_SET, DeliveryGuarantee.EXACTLY_ONCE,
                                                                        MapReducer.getInstance(), FINALZER);
      Map<Integer, Integer> results = stage.toCompletableFuture().join();
      assertEquals(threadKeys.size(), results.size());
      for (Integer key : threadKeys) {
         assertEquals(key, results.get(key));
      }
   }

   private static final Function<Publisher<Map<Integer, Integer>>, CompletionStage<Map<Integer, Integer>>> FINALZER =
         p -> Flowable.fromPublisher(p)
               .reduce((map1, map2) -> {
                  map1.putAll(map2);
                  return map1;
               }).toCompletionStage();

   public static class MapReducer<K, V> implements Function<Publisher<Map.Entry<K, V>>, CompletionStage<Map<K, V>>> {
      private static final MapReducer INSTANCE = new MapReducer();

      @ProtoFactory
      public static MapReducer protoFactory() {
         return INSTANCE;
      }

      public static <K, V> Function<Publisher<? extends Map.Entry<K, V>>, CompletionStage<Map<K, V>>> getInstance() {
         return INSTANCE;
      }

      @Override
      public CompletionStage<Map<K, V>> apply(Publisher<Map.Entry<K, V>> entryPublisher) {
         Map<K, V> startingMap = new HashMap<>();
         return Flowable.fromPublisher(entryPublisher)
               .collectInto(startingMap, (map, e) ->
                     map.put(e.getKey(), e.getValue())).toCompletionStage();
      }
   }

   @ProtoSchema(
         includeClasses = MapReducer.class,
         schemaFileName = "test.core.PublisherManagerGetKeyStressTest.proto",
         schemaFilePath = "proto/generated",
         schemaPackageName = "org.infinispan.test.core.PublisherManagerGetKeyStressTest",
         service = false,
         syntax = ProtoSyntax.PROTO3
   )
   interface PublisherManagerGetKeyStressTestSCI extends SerializationContextInitializer {
      SerializationContextInitializer INSTANCE = new PublisherManagerGetKeyStressTestSCIImpl();
   }
}
