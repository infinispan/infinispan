package org.infinispan.persistence.jdbc.binary;

import org.infinispan.Cache;
import org.infinispan.commons.io.ByteBufferFactoryImpl;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.marshall.core.MarshalledEntryFactoryImpl;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.InitializationContextImpl;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.persistence.jdbc.TableManipulation;
import org.infinispan.persistence.jdbc.configuration.JdbcBinaryStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.infinispan.util.DefaultTimeService;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.Serializable;

import static org.infinispan.test.TestingUtil.metadata;
import static org.mockito.Mockito.mock;

/**
 * Tester class for {@link JdbcBinaryStore}
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "persistence.jdbc.binary.JdbcBinaryStoreTest")
public class JdbcBinaryStoreTest extends BaseStoreTest {


   private EmbeddedCacheManager cacheManager;
   private Cache<Object,Object> cache;

   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {

      ConfigurationBuilder cc = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      JdbcBinaryStoreConfigurationBuilder storeBuilder = cc
            .persistence()
            .addStore(JdbcBinaryStoreConfigurationBuilder.class);
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.table(), true);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(storeBuilder);

      cacheManager = TestCacheManagerFactory.createCacheManager(cc);

      cache = cacheManager.getCache();

      JdbcBinaryStore jdbcBinaryCacheStore = TestingUtil.getFirstWriter(cache);
      assert jdbcBinaryCacheStore.getConnectionFactory() != null;
      csc = jdbcBinaryCacheStore.getConfiguration();
      return jdbcBinaryCacheStore;
   }

   @AfterMethod
   @Override
   public void tearDown() throws PersistenceException {
      super.tearDown();
      TestingUtil.killCacheManagers(cacheManager);
   }

   @Override
   protected StreamingMarshaller getMarshaller() {
      StreamingMarshaller component = cache.getAdvancedCache().getComponentRegistry().getCacheMarshaller();
      assert component != null;
      return component;
   }

   public void testNotCreateConnectionFactory() throws Exception {
      JdbcBinaryStoreConfigurationBuilder storeBuilder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .persistence()
               .addStore(JdbcBinaryStoreConfigurationBuilder.class)
                  .manageConnectionFactory(false);

      storeBuilder.table().createOnStart(false);

      JdbcBinaryStore jdbcBucketCacheStore = new JdbcBinaryStore();
      jdbcBucketCacheStore.init(new InitializationContextImpl(storeBuilder.create(), getCache(), getMarshaller(),
                                                              new DefaultTimeService(), new ByteBufferFactoryImpl(),
                                                              new MarshalledEntryFactoryImpl(getMarshaller())));
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
      UnitTestDatabaseManager.verifyConnectionLeaks(((JdbcBinaryStore) cl).getConnectionFactory());
   }

   public void testPurgeExpiredAllCodepaths() throws Exception {
      FixedHashKey k1 = new FixedHashKey(1, "a");
      FixedHashKey k2 = new FixedHashKey(1, "b");
      cl.write(new MarshalledEntryImpl(k1, "value", null, getMarshaller()));
      Metadata metadata = metadata(1000, null);
      InternalMetadataImpl im = new InternalMetadataImpl(metadata, System.currentTimeMillis(), System.currentTimeMillis());
      cl.write(new MarshalledEntryImpl(k2, "value", im, getMarshaller())); // will expire
      for (int i = 0; i < 120; i++) {
         cl.write(new MarshalledEntryImpl(new FixedHashKey(i + 10, "non-exp k" + i), "value", null, getMarshaller()));
         cl.write(new MarshalledEntryImpl(new FixedHashKey(i + 10, "exp k" + i), "value", im, getMarshaller())); // will expire
      }
      TestingUtil.sleepThread(1000);
      assert cl.contains(k1);
      assert !cl.contains(k2);
      (cl).purge(new WithinThreadExecutor(), null);
      assert cl.contains(k1);
      assert !cl.contains(k2);
      UnitTestDatabaseManager.verifyConnectionLeaks(((JdbcBinaryStore) cl).getConnectionFactory());
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
