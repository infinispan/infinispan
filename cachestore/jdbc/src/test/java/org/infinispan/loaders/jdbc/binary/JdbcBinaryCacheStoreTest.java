package org.infinispan.loaders.jdbc.binary;

import static org.mockito.Mockito.mock;

import java.io.Serializable;

import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.configuration.JdbcBinaryCacheStoreConfigurationBuilder;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.loaders.spi.CacheStore;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

/**
 * Tester class for {@link JdbcBinaryCacheStore}
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "loaders.jdbc.binary.JdbcBinaryCacheStoreTest")
public class JdbcBinaryCacheStoreTest extends BaseCacheStoreTest {

   @Override
   protected CacheStore createCacheStore() throws Exception {
      JdbcBinaryCacheStoreConfigurationBuilder storeBuilder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .loaders()
               .addLoader(JdbcBinaryCacheStoreConfigurationBuilder.class)
                  .purgeSynchronously(true);

      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.table(), true);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(storeBuilder);

      JdbcBinaryCacheStore jdbcBucketCacheStore = new JdbcBinaryCacheStore();
      jdbcBucketCacheStore.init(storeBuilder.create(), getCache(), getMarshaller());
      jdbcBucketCacheStore.start();
      assert jdbcBucketCacheStore.getConnectionFactory() != null;
      return jdbcBucketCacheStore;
   }

   public void testNotCreateConnectionFactory() throws Exception {
      JdbcBinaryCacheStoreConfigurationBuilder storeBuilder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .loaders()
               .addLoader(JdbcBinaryCacheStoreConfigurationBuilder.class)
                  .purgeSynchronously(true)
                  .manageConnectionFactory(false);

      storeBuilder.table().createOnStart(false);

      JdbcBinaryCacheStore jdbcBucketCacheStore = new JdbcBinaryCacheStore();
      jdbcBucketCacheStore.init(storeBuilder.create(), getCache(), new TestObjectStreamMarshaller());
      jdbcBucketCacheStore.start();
      assert jdbcBucketCacheStore.getConnectionFactory() == null;

      /* this will make sure that if a method like stop is called on the connection then it will barf an exception */
      ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
      TableManipulation tableManipulation = mock(TableManipulation.class);

      tableManipulation.start(connectionFactory);
      tableManipulation.setCacheName("aName");
      jdbcBucketCacheStore.doConnectionFactoryInitialization(connectionFactory);

      //stop should be called even if this is an externally managed connection
      tableManipulation.stop();
      jdbcBucketCacheStore.stop();
   }



   @Override
   public void testPurgeExpired() throws Exception {
      super.testPurgeExpired();
      UnitTestDatabaseManager.verifyConnectionLeaks(((JdbcBinaryCacheStore)cs).getConnectionFactory());
   }

   public void testPurgeExpiredAllCodepaths() throws Exception {
      FixedHashKey k1 = new FixedHashKey(1, "a");
      FixedHashKey k2 = new FixedHashKey(1, "b");
      cs.store(TestInternalCacheEntryFactory.create(k1, "value"));
      cs.store(TestInternalCacheEntryFactory.create(k2, "value", 1000)); // will expire
      for (int i = 0; i < 120; i++) {
         cs.store(TestInternalCacheEntryFactory.create(new FixedHashKey(i + 10, "non-exp k" + i), "value"));
         cs.store(TestInternalCacheEntryFactory.create(new FixedHashKey(i + 10, "exp k" + i), "value", 1000)); // will expire
      }
      TestingUtil.sleepThread(1000);
      assert cs.containsKey(k1);
      assert !cs.containsKey(k2);
      cs.purgeExpired();
      assert cs.containsKey(k1);
      assert !cs.containsKey(k2);
      UnitTestDatabaseManager.verifyConnectionLeaks(((JdbcBinaryCacheStore)cs).getConnectionFactory());
   }

   private static final class FixedHashKey implements Serializable {
      String s;
      int i;

      private FixedHashKey(int i, String s) {
         this.s = s;
         this.i = i;
      }

      @Override
      public int hashCode() {
         return i;
      }

      @Override
      public boolean equals(Object other) {
         return other instanceof FixedHashKey && s.equals(((FixedHashKey) other).s);
      }
   }

}
