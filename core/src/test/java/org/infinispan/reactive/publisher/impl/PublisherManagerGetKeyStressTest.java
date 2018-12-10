package org.infinispan.reactive.publisher.impl;

import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.commands.GetAllCommandStressTest;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.reactive.RxJavaInterop;
import org.infinispan.test.fwk.InCacheMode;
import org.reactivestreams.Publisher;
import org.testng.annotations.Test;

import io.reactivex.Flowable;

@Test(groups = "stress", testName = "PublisherManagerGetKeyStressTest", timeOut = 15*60*1000)
@InCacheMode(CacheMode.DIST_SYNC)
public class PublisherManagerGetKeyStressTest extends GetAllCommandStressTest {

   @Override
   protected void workerLogic(Cache<Integer, Integer> cache, Set<Integer> threadKeys, int iteration) {
      ClusterPublisherManager<Integer, Integer> cpm = cache.getAdvancedCache().getComponentRegistry().getComponent(ClusterPublisherManager.class);
      CompletionStage<Map<Integer, Integer>> stage = cpm.entryReduction(false, null, threadKeys, null, false, DeliveryGuarantee.EXACTLY_ONCE,
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
               }).to(RxJavaInterop.maybeToCompletionStage());

   @SerializeWith(value = MapReducer.MapReducerExternalizer.class)
   private static class MapReducer<K, V> implements Function<Publisher<Map.Entry<K, V>>, CompletionStage<Map<K, V>>> {
      private static final MapReducer INSTANCE = new MapReducer();

      public static <K, V> Function<Publisher<? extends Map.Entry<K, V>>, CompletionStage<Map<K, V>>> getInstance() {
         return INSTANCE;
      }

      @Override
      public CompletionStage<Map<K, V>> apply(Publisher<Map.Entry<K, V>> entryPublisher) {
         Map<K, V> startingMap = new HashMap<>();
         return Flowable.fromPublisher(entryPublisher)
               .collectInto(startingMap, (map, e) ->
                     map.put(e.getKey(), e.getValue())).to(RxJavaInterop.singleToCompletionStage());
      }

      static final class MapReducerExternalizer implements Externalizer<MapReducer> {
         @Override
         public void writeObject(ObjectOutput output, MapReducer object) throws IOException { }

         @Override
         public MapReducer readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return MapReducer.INSTANCE;
         }
      }
   }
}
