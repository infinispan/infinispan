package org.infinispan.persistence.jdbc.binary;

import static org.infinispan.test.TestingUtil.metadata;
import static org.mockito.Mockito.mock;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.io.Serializable;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.marshall.core.ExternalPojo;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.jdbc.DatabaseType;
import org.infinispan.persistence.jdbc.configuration.JdbcBinaryStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.table.management.TableManager;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.Test;

/**
 * Tester class for {@link JdbcBinaryStore}
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "persistence.jdbc.binary.JdbcBinaryStoreTest")
public class JdbcBinaryStoreTest extends BaseStoreTest {

   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {

      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      JdbcBinaryStoreConfigurationBuilder storeBuilder = builder
            .persistence()
            .addStore(JdbcBinaryStoreConfigurationBuilder.class);
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.table(), true);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(storeBuilder);

      JdbcBinaryStore jdbcBinaryCacheStore = new JdbcBinaryStore();
      jdbcBinaryCacheStore.init(createContext(builder.build()));
      return jdbcBinaryCacheStore;
   }

   public void testNotCreateConnectionFactory() throws Exception {
      ConfigurationBuilder builder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false);
      JdbcBinaryStoreConfigurationBuilder storeBuilder = builder
            .persistence()
            .addStore(JdbcBinaryStoreConfigurationBuilder.class)
            .manageConnectionFactory(false)
            .dialect(DatabaseType.H2)
            .dbMajorVersion(1)
            .dbMinorVersion(4);

      storeBuilder.table().createOnStart(false);

      JdbcBinaryStore jdbcBucketCacheStore = new JdbcBinaryStore();
      jdbcBucketCacheStore.init(createContext(builder.build()));
      jdbcBucketCacheStore.start();
      assertNull(jdbcBucketCacheStore.getConnectionFactory());

      /* this will make sure that if a method like stop is called on the connection then it will barf an exception */
      ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
      TableManager tableManager = mock(TableManager.class);

      tableManager.start();
      tableManager.setCacheName("aName");
      jdbcBucketCacheStore.initializeConnectionFactory(connectionFactory);

      //stop should be called even if this is an externally managed connection
      tableManager.stop();
      jdbcBucketCacheStore.stop();
   }



   @Override
   public void testPurgeExpired() throws Exception {
      super.testPurgeExpired();
      UnitTestDatabaseManager.verifyConnectionLeaks(((JdbcBinaryStore) cl).getConnectionFactory());
   }

   public void testPurgeExpiredAllCodepaths() throws Exception {
      FixedHashKey k1 = new FixedHashKey(1, "a");
      FixedHashKey k2 = new FixedHashKey(1, "b");
      cl.write(marshalledEntry(k1, "value", null));
      Metadata metadata = metadata(1000, null);
      InternalMetadataImpl im = new InternalMetadataImpl(metadata, timeService.wallClockTime(), timeService.wallClockTime());
      cl.write(marshalledEntry(k2, "value", im)); // will expire
      for (int i = 0; i < 120; i++) {
         cl.write(marshalledEntry(new FixedHashKey(i + 10, "non-exp k" + i), "value", null));
         cl.write(marshalledEntry(new FixedHashKey(i + 10, "exp k" + i), "value", im)); // will expire
      }
      timeService.advance(1001);
      assertContains(k1, true);
      assertContains(k2, false);
      cl.purge(new WithinThreadExecutor(), null);
      assertContains(k1, true);
      assertContains(k2, false);
      UnitTestDatabaseManager.verifyConnectionLeaks(((JdbcBinaryStore) cl).getConnectionFactory());
   }


   public void testCacheSize() {
      long lifespan = 3000;
      for (int i = 0; i < 240; i++) {
         if (i % 2 == 0) {
            cl.write(marshalledEntry(internalCacheEntry("key" + i, "v1", lifespan)));
         } else {
            cl.write(marshalledEntry(internalCacheEntry("key" + i, "v1", -1)));
         }
      }
      assertEquals(cl.size(), 240);
      timeService.advance(lifespan + 1);
      assertEquals(cl.size(), 120);

   }

   private static final class FixedHashKey implements Serializable, ExternalPojo {
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

      @Override
      public String toString() {
         return "FixedHashKey{" +
               "key='" + s + '\'' +
               ", hashCode=" + i +
               '}';
      }
   }

}
