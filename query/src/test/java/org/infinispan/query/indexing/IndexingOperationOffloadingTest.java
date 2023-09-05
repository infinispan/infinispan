package org.infinispan.query.indexing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.logging.Log;
import org.infinispan.query.model.TypeA;
import org.infinispan.query.model.TypeB;
import org.infinispan.query.model.TypeC;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.indexing.IndexingOperationOffloadingTest")
public class IndexingOperationOffloadingTest extends SingleCacheManagerTest {

   private static final Log log = LogFactory.getLog(IndexingOperationOffloadingTest.class, Log.class);

   private static final String CACHE_NAME = "types";

   private static final int SIZE = 400;
   private static final int CHUNK_SIZE = 20;
   private static final int CHUNKS_NUMBER = SIZE / CHUNK_SIZE;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config
            .indexing()
            .enable()
            .storage(IndexStorage.LOCAL_HEAP)
            .writer().queueCount(1).queueSize(1) // Use a very small single queue
            .addIndexedEntity(TypeA.class)
            .addIndexedEntity(TypeB.class)
            .addIndexedEntity(TypeC.class);

      EmbeddedCacheManager result = TestCacheManagerFactory.createCacheManager();
      result.defineConfiguration(CACHE_NAME, config.build());

      return result;
   }

   public void putAll() throws Exception {
      Cache<Object, Object> types = cacheManager.getCache(CACHE_NAME);
      HashMap<String, Object> entries = new HashMap<>();

      for (int i = 0; i < SIZE; i++) {
         String id = "simple-" + i;
         String key = "key-" + id;
         String value = "value-" + id;

         entries.put(key, new TypeA(value));
      }

      types.putAllAsync(entries).get();
      assertThat(entries).hasSize(SIZE);

      Query<TypeA> queryAll = types.query("from org.infinispan.query.model.TypeA");
      QueryResult<TypeA> result = queryAll.execute();
      assertThat(result.count().value()).isEqualTo(SIZE);
   }

   @Test
   public void batchedPutAll() throws Exception {
      Cache<Object, Object> types = cacheManager.getCache(CACHE_NAME);
      CompletableFuture<Void>[] chunksExecutions = new CompletableFuture[CHUNKS_NUMBER];
      AtomicInteger completedExecutions = new AtomicInteger(0);
      long timeZero = System.currentTimeMillis();

      for (int c = 0; c < CHUNKS_NUMBER; c++) {
         HashMap<String, Object> chunk = new HashMap<>();

         for (int i = 0; i < CHUNK_SIZE; i++) {
            String id = "batch-" + c + "-" + i;
            String key = "key-" + id;
            String value = "value-" + id;

            chunk.put(key, new TypeB(value));
         }

         CompletableFuture<Void> execution = types.putAllAsync(chunk);
         log.info("Started: " + (c + 1) + " / " + CHUNKS_NUMBER + ". Elapsed: " + getElapsed(timeZero));

         execution.whenComplete((unused, throwable) -> {
            if (throwable != null) {
               fail("We don't expect the throwable:", throwable);
            }
            log.info("Completed: " + completedExecutions.incrementAndGet() + " / " + CHUNKS_NUMBER +
                  ". Elapsed: " + getElapsed(timeZero));
         });
         chunksExecutions[c] = execution;
      }

      CompletableFuture.allOf(chunksExecutions).get();
      assertThat(completedExecutions.get()).isEqualTo(CHUNKS_NUMBER);

      Query<TypeB> queryAll = types.query("from org.infinispan.query.model.TypeB");
      QueryResult<TypeB> result = queryAll.execute();
      assertThat(result.count().value()).isEqualTo(CHUNKS_NUMBER * CHUNK_SIZE);
   }

   private long getElapsed(long timeZero) {
      return System.currentTimeMillis() - timeZero;
   }
}
