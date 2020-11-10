package org.infinispan.query.tx;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.query.helper.TestQueryHelperFactory.createCacheQuery;
import static org.infinispan.test.TestingUtil.withTx;

import java.util.concurrent.Callable;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.tx.TransactionalQueryTest")
public class TransactionalQueryTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
         .indexing()
            .enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Session.class);
      return TestCacheManagerFactory.createCacheManager(new SCIImpl(), cfg);
   }

   @BeforeMethod
   public void initialize() throws Exception {
      // Initialize the cache
      withTx(tm(), (Callable<Void>) () -> {
         for (int i = 0; i < 100; i++) {
            cache.put(String.valueOf(i), new Session(String.valueOf(i)));
         }
         return null;
      });
   }

   public void run() throws Exception {
      // Verify querying works
      createCacheQuery(Session.class, cache, "Id", "2");

      // Remove something that exists
      withTx(tm(), (Callable<Void>) () -> {
         cache.remove("50");
         return null;
      });

      // Remove something that doesn't exist with a transaction
      // This also fails without using a transaction
      withTx(tm(), (Callable<Void>) () -> {
         cache.remove("200");
         return null;
      });
   }

   @Indexed(index = "SessionIndex")
   public static class Session {
      private String m_id;

      @ProtoFactory
      Session(String id) {
         m_id = id;
      }

      @Field(name = "Id")
      @ProtoField(number = 1)
      public String getId() {
         return m_id;
      }
   }

   @AutoProtoSchemaBuilder(
         includeClasses = Session.class,
         schemaFileName = "test.query.tx.TransactionalQueryTest.proto",
         schemaFilePath = "proto/generated",
         schemaPackageName = "org.infinispan.test.TransactionalQueryTest",
         service = false
   )
   interface SCI extends SerializationContextInitializer {
   }
}
