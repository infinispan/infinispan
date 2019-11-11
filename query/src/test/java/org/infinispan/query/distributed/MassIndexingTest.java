package org.infinispan.query.distributed;


import static org.testng.Assert.assertEquals;

import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.context.Flag;
import org.infinispan.functional.FunctionalTestUtils;
import org.infinispan.query.MassIndexer;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.api.NotIndexedType;
import org.infinispan.query.impl.massindex.MassIndexerAlreadyStartedException;
import org.infinispan.query.queries.faceting.Car;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Running mass indexer on big bunch of data.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.distributed.MassIndexingTest")
public class MassIndexingTest extends DistributedMassIndexingTest {

   public void testReindexing() throws Exception {
      for (int i = 0; i < 200; i++) {
         caches.get(i % 2).getAdvancedCache().withFlags(Flag.SKIP_INDEXING).put(key("F" + i + "NUM"),
               new Car((i % 2 == 0 ? "megane" : "bmw"), "blue", 300 + i));
      }

      //Adding also non-indexed values
      caches.get(0).getAdvancedCache().put(key("FNonIndexed1NUM"), new NotIndexedType("test1"));
      caches.get(0).getAdvancedCache().put(key("FNonIndexed2NUM"), new NotIndexedType("test2"));

      verifyFindsCar(0, "megane");
      verifyFindsCar(0, "test1");
      verifyFindsCar(0, "test2");

      caches.get(0).getAdvancedCache().withFlags(Flag.SKIP_INDEXING).put(key("FNonIndexed3NUM"), new NotIndexedType("test3"));
      verifyFindsCar(0, "test3");

      //re-sync datacontainer with indexes:
      rebuildIndexes();

      verifyFindsCar(100, "megane");
      verifyFindsCar(0, "test1");
      verifyFindsCar(0, "test2");
   }

   public void testOverlappingMassIndexers() {
      Cache<Integer, Car> cache = caches.get(0);
      SearchManager searchManager = Search.getSearchManager(cache);
      MassIndexer massIndexer = searchManager.getMassIndexer();

      IntStream.range(0, 300).forEach(i -> cache.put(i, new Car("whatever", "whatever", 0)));

      CompletableFuture<Void> first = massIndexer.startAsync();
      eventually(massIndexer::isRunning);

      CompletableFuture<Void> second = massIndexer.startAsync();

      assertSuccessCompletion(first);
      assertErrorCompletion(second, MassIndexerAlreadyStartedException.class);
      eventually(() -> !massIndexer.isRunning());

      CompletableFuture<Void> third = massIndexer.startAsync();

      assertSuccessCompletion(third);
   }

   private void assertSuccessCompletion(CompletableFuture<Void> future) {
      try {
         FunctionalTestUtils.await(future);
      } catch (Exception e) {
         Assert.fail("Future should've completed successfully");
      }
   }

   private void assertErrorCompletion(CompletableFuture<Void> future, Class<? extends Throwable> expected) {
      try {
         FunctionalTestUtils.await(future);
         Assert.fail("Future should've thrown an error");
      } catch (Error e) {
         assertEquals(Util.getRootCause(e).getClass(), expected);
      }
   }

   @Override
   protected void rebuildIndexes() throws Exception {
      Cache cache = caches.get(0);
      SearchManager searchManager = Search.getSearchManager(cache);
      CompletableFuture<Void> future = searchManager.getMassIndexer().startAsync();
      future.get();
   }
}
