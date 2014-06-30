package org.infinispan.query.tx;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;

import static org.infinispan.query.helper.TestQueryHelperFactory.*;
import static org.infinispan.test.TestingUtil.withTx;

@Test(groups = "functional", testName = "query.tx.TransactionalQueryTest")
public class TransactionalQueryTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
         .indexing()
            .index(Index.ALL)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @BeforeMethod
   public void initialize() throws Exception {
      // Initialize the cache
      withTx(tm(), new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            for (int i = 0; i < 100; i++) {
               cache.put(String.valueOf(i), new Session(String.valueOf(i)));
            }
            return null;
         }
      });
   }

   public void run() throws Exception {
      // Verify querying works
      createCacheQuery(cache, "", "Id:2?");

      // Remove something that exists
      withTx(tm(), new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache.remove("50");
            return null;
         }
      });

      // Remove something that doesn't exist with a transaction
      // This also fails without using a transaction
      withTx(tm(), new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache.remove("200");
            return null;
         }
      });
   }

   @Indexed(index = "SessionIndex")
   public static class Session {
      private String m_id;

      public Session(String id) {
         m_id = id;
      }

      @Field(name = "Id")
      public String getId() {
         return m_id;
      }
   }
}
