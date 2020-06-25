package org.infinispan.query.distributed;


import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertFalse;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.context.Flag;
import org.infinispan.functional.FunctionalTestUtils;
import org.infinispan.query.Indexer;
import org.infinispan.query.Search;
import org.infinispan.query.api.NotIndexedType;
import org.infinispan.query.impl.massindex.IndexUpdater;
import org.infinispan.query.impl.massindex.MassIndexerAlreadyStartedException;
import org.infinispan.query.queries.faceting.Car;
import org.infinispan.test.TestingUtil;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * Running mass indexer on big bunch of data.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.distributed.MassIndexingTest")
public class MassIndexingTest extends DistributedMassIndexingTest {

   public void testReindexing() {
      for (int i = 0; i < 10; i++) {
         cache(i % 2).getAdvancedCache().withFlags(Flag.SKIP_INDEXING).put(key("F" + i + "NUM"),
               new Car((i % 2 == 0 ? "megane" : "bmw"), "blue", 300 + i));
      }

      //Adding also non-indexed values
      cache(0).getAdvancedCache().put(key("FNonIndexed1NUM"), new NotIndexedType("test1"));
      cache(0).getAdvancedCache().put(key("FNonIndexed2NUM"), new NotIndexedType("test2"));

      verifyFindsCar(0, "megane");
      verifyFindsCar(0, "test1");
      verifyFindsCar(0, "test2");

      cache(0).getAdvancedCache().withFlags(Flag.SKIP_INDEXING).put(key("FNonIndexed3NUM"), new NotIndexedType("test3"));
      verifyFindsCar(0, "test3");

      //re-sync datacontainer with indexes:
      rebuildIndexes();

      verifyFindsCar(5, "megane");
      verifyFindsCar(0, "test1");
      verifyFindsCar(0, "test2");
   }

   @Test
   public void testOverlappingMassIndexers() {
      Cache<Integer, Car> cache = cache(0);
      IntStream.range(0, 10).forEach(i -> cache.put(i, new Car("whatever", "whatever", 0)));

      Indexer massIndexer = Search.getIndexer(cache);

      CountDownLatch latch = new CountDownLatch(1);
      instrumentIndexer(massIndexer, latch);

      CompletionStage<Void> first = massIndexer.run();
      CompletionStage<Void> second = massIndexer.run();

      latch.countDown();

      assertTrue(isSuccess(second) && isError(first) || isSuccess(first) && isError(second));
      assertFalse(massIndexer.isRunning());

      CompletionStage<Void> third = massIndexer.run();

      assertTrue(isSuccess(third));
   }

   private void instrumentIndexer(Indexer original, CountDownLatch latch) {
      TestingUtil.replaceField(original, "indexUpdater", (Function<IndexUpdater, IndexUpdater>) indexUpdater -> {
         IndexUpdater mock = Mockito.spy(indexUpdater);
         Mockito.doAnswer(invocation -> {
            latch.await();
            return invocation.callRealMethod();
         }).when(mock).flush(any());
         return mock;
      });
   }

   public boolean isSuccess(CompletionStage<Void> future) {
      try {
         FunctionalTestUtils.await(future);
         return true;
      } catch (Throwable e) {
         return false;
      }
   }

   private boolean isError(CompletionStage<Void> future) {
      try {
         FunctionalTestUtils.await(future);
         return false;
      } catch (Throwable e) {
         return Util.getRootCause(e).getClass().equals(MassIndexerAlreadyStartedException.class);
      }
   }

   @Override
   protected void rebuildIndexes() {
      Cache<?, ?> cache = cache(0);
      join(Search.getIndexer(cache).run());
   }
}
