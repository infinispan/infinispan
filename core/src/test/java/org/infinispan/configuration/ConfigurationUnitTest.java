package org.infinispan.configuration;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createCacheManager;
import static org.infinispan.transaction.TransactionMode.NON_TRANSACTIONAL;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.net.URL;

import javax.xml.XMLConstants;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.SchemaFactory;

import org.infinispan.Cache;
import org.infinispan.Version;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.util.FileLookup;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "configuration.ConfigurationUnitTest")
public class ConfigurationUnitTest extends AbstractInfinispanTest {

   @Test
   public void testBuild() {
      // Simple test to ensure we can actually build a config
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.build();
   }

   @Test
   public void testCreateCache() {
      withCacheManager(new CacheManagerCallable(createCacheManager()));
   }

   @Test
   public void testEvictionSize() {
      Configuration configuration = new ConfigurationBuilder()
         .memory().size(20)
         .build();
      Assert.assertEquals(configuration.memory().size(), 20);
   }

   @Test
   public void testDistSyncAutoCommit() {
      Configuration configuration = new ConfigurationBuilder()
         .clustering().cacheMode(CacheMode.DIST_SYNC)
         .transaction().autoCommit(true)
         .build();
      Assert.assertTrue(configuration.transaction().autoCommit());
      Assert.assertEquals(configuration.clustering().cacheMode(), CacheMode.DIST_SYNC);
   }

   @Test
   public void testDummyTMGetCache() throws Exception {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.transaction().use1PcForAutoCommitTransactions(true)
            .transactionManagerLookup(new EmbeddedTransactionManagerLookup());
      withCacheManager(new CacheManagerCallable(createCacheManager()) {
         @Override
         public void call() {
            cm.getCache();
         }
      });
   }

   @Test
   public void testGetCache() {
      withCacheManager(new CacheManagerCallable(createCacheManager()) {
         @Override
         public void call() {
            cm.getCache();
         }
      });
   }

   @Test
   public void testDefineNamedCache() {
      withCacheManager(new CacheManagerCallable(createCacheManager()) {
         @Override
         public void call() {
            cm.defineConfiguration("foo", new ConfigurationBuilder().build());
         }
      });
   }

   @Test
   public void testGetAndPut() throws Exception {
      withCacheManager(new CacheManagerCallable(createCacheManager()) {
         @Override
         public void call() {
            Cache<String, String> cache = cm.getCache();
            cache.put("Foo", "2");
            cache.put("Bar", "4");
            Assert.assertEquals(cache.get("Foo"), "2");
            Assert.assertEquals(cache.get("Bar"), "4");
         }
      });
   }

   @Test(expectedExceptions = CacheConfigurationException.class,
         expectedExceptionsMessageRegExp = "ISPN(\\d)*: Cannot enable Invocation Batching when the Transaction Mode is NON_TRANSACTIONAL, set the transaction mode to TRANSACTIONAL")
   public void testInvocationBatchingAndNonTransactional() throws Exception {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.transaction()
            .transactionMode(NON_TRANSACTIONAL)
            .invocationBatching()
            .enable();
      withCacheManager(new CacheManagerCallable(createCacheManager(cb)));
   }

   @Test
   public void testDisableL1() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createClusteredCacheManager(
                  new ConfigurationBuilder(), new TransportFlags())) {
         @Override
         public void call() {
            ConfigurationBuilder cb = new ConfigurationBuilder();
            cb.clustering().cacheMode(CacheMode.DIST_SYNC).l1().disable();
            cm.defineConfiguration("testConfigCache", cb.build());
            Cache<Object, Object> cache = cm.getCache("testConfigCache");
            assert !cache.getCacheConfiguration().clustering().l1().enabled();
         }
      });
   }

   @Test
   public void testClearStores() {
      Configuration c = new ConfigurationBuilder()
            .persistence()
               .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .persistence()
               .clearStores()
         .build();
      assertEquals(c.persistence().stores().size(), 0);
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testClusterNameNull() {
      GlobalConfigurationBuilder gc = new GlobalConfigurationBuilder();
      gc.transport().clusterName(null).build();
   }

   @Test
   public void testSchema() throws Exception {
      FileLookup lookup = FileLookupFactory.newInstance();
      String schemaFilename = String.format("schema/infinispan-config-%s.xsd", Version.getMajorMinor());
      URL schemaFile = lookup.lookupFileLocation(schemaFilename, Thread.currentThread().getContextClassLoader());
      if (schemaFile == null) {
         throw new NullPointerException("Failed to find a schema file " + schemaFilename);
      }
      Source xmlFile = new StreamSource(lookup.lookupFile(String.format("configs/unified/%s.xml", Version.getMajorMinor()), Thread.currentThread().getContextClassLoader()));
      SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI).newSchema(schemaFile).newValidator().validate(xmlFile);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testNumOwners() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      cb.clustering().hash().numOwners(5);

      Configuration c = cb.build();
      Assert.assertEquals(5, c.clustering().hash().numOwners());

      // negative test
      cb.clustering().hash().numOwners(0);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void numVirtualNodes() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      cb.clustering().hash().numSegments(5);

      Configuration c = cb.build();
      Assert.assertEquals(5, c.clustering().hash().numSegments());

      // negative test
      cb.clustering().hash().numSegments(0);
   }

   public void testNoneIsolationLevel() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.locking().isolationLevel(IsolationLevel.NONE);
      withCacheManager(new CacheManagerCallable(
            createCacheManager(builder)) {
         @Override
         public void call() {
            Configuration cfg = cm.getCache().getCacheConfiguration();
            assertEquals(IsolationLevel.NONE, cfg.locking().isolationLevel());
         }
      });
   }

   public void testNoneIsolationLevelInCluster() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.locking().isolationLevel(IsolationLevel.NONE)
            .clustering().cacheMode(CacheMode.REPL_SYNC).build();
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createClusteredCacheManager(builder)) {
         @Override
         public void call() {
            Configuration cfg = cm.getCache().getCacheConfiguration();
            assertEquals(IsolationLevel.READ_COMMITTED,
                  cfg.locking().isolationLevel());
         }
      });
   }

   public void testConfigureMarshaller() {
      GlobalConfigurationBuilder gc = new GlobalConfigurationBuilder();
      TestObjectStreamMarshaller marshaller = new TestObjectStreamMarshaller();
      gc.serialization().marshaller(marshaller);
      withCacheManager(new CacheManagerCallable(
            createCacheManager(gc, new ConfigurationBuilder())) {
         @Override
         public void call() {
            cm.getCache();
         }
      });
      marshaller.stop();
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testWrongCacheModeConfiguration() throws Exception {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.clustering().cacheMode(CacheMode.REPL_ASYNC);
      TestCacheManagerFactory.createCacheManager(config);
   }

   public void testCacheModeConfiguration() throws Exception {
      withCacheManager(new CacheManagerCallable(createTestCacheManager()) {
         @Override
         public void call() {
            cm.getCache("local").put("key", "value");
         }
      });
   }

   private EmbeddedCacheManager createTestCacheManager() {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.clustering().cacheMode(CacheMode.REPL_ASYNC);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(config);
      config = new ConfigurationBuilder();
      cm.defineConfiguration("local", config.build());
      return cm;
   }

   @Test(expectedExceptions = CacheConfigurationException.class,
         expectedExceptionsMessageRegExp = "ISPN(\\d)*: Indexing can not be enabled on caches in Invalidation mode")
   public void testIndexingOnInvalidationCache() {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.clustering().cacheMode(CacheMode.INVALIDATION_SYNC);
      c.indexing().index(Index.ALL);
      c.validate();
   }

   @Test(expectedExceptions = CacheConfigurationException.class, expectedExceptionsMessageRegExp =
         "ISPN(\\d)*: Indexing can only be enabled if infinispan-query.jar is available on your classpath, and this jar has not been detected.")
   public void testIndexingRequiresOptionalModule() {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.indexing().index(Index.ALL);
      c.validate(GlobalConfigurationBuilder.defaultClusteredBuilder().build());
   }

   @Test(expectedExceptions = CacheConfigurationException.class,
         expectedExceptionsMessageRegExp = "ISPN(\\d)*: A cache configured with invocation batching can't have recovery enabled")
   public void testInvalidBatchingAndTransactionConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.invocationBatching().enable();
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      builder.transaction().useSynchronization(false);
      builder.transaction().recovery().enable();
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(builder)) {
         @Override
         public void call() {
            cm.getCache();
         }
      });
   }

   @Test(expectedExceptions = CacheConfigurationException.class,
         expectedExceptionsMessageRegExp = "ISPN(\\d)*: Recovery not supported with non transactional cache")
   public void testInvalidRecoveryWithNonTransactional() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
      builder.transaction().useSynchronization(false);
      builder.transaction().recovery().enable();
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(builder)) {
         @Override
         public void call() {
            cm.getCache();
         }
      });
   }

   @Test(expectedExceptions = CacheConfigurationException.class,
         expectedExceptionsMessageRegExp = "ISPN(\\d)*: Recovery not supported with Synchronization")
   public void testInvalidRecoveryWithSynchronization() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      builder.transaction().useSynchronization(true);
      builder.transaction().recovery().enable();
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(builder)) {
         @Override
         public void call() {
            cm.getCache();
         }
      });
   }

   public void testValidRecoveryConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      builder.transaction().useSynchronization(false);
      builder.transaction().recovery().enable();
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(builder)) {
         @Override
         public void call() {
            AssertJUnit.assertTrue(cm.getCache().getCacheConfiguration().transaction().recovery().enabled());
         }
      });
   }

   public void testMultipleValidationErrors() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.transaction().reaperWakeUpInterval(-1);
      builder.modules().add(new NonValidatingBuilder());
      try {
         builder.validate();
         fail("Expected CacheConfigurationException");
      } catch (CacheConfigurationException e) {
         assertTrue(e.getMessage().startsWith("ISPN000919"));
         assertEquals(e.getSuppressed().length, 2);
         assertTrue(e.getSuppressed()[0].getMessage().startsWith("ISPN000344"));
         assertEquals("MODULE ERROR", e.getSuppressed()[1].getMessage());
      }

      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      global.security().authorization().enable();
      global.modules().add(new NonValidatingBuilder());
      try {
         global.validate();
         fail("Expected CacheConfigurationException");
      } catch (CacheConfigurationException e) {
         assertTrue(e.getMessage().startsWith("ISPN000919"));
         assertEquals(e.getSuppressed().length, 2);
         assertTrue(e.getSuppressed()[0].getMessage().startsWith("ISPN000288"));
         assertEquals("MODULE ERROR", e.getSuppressed()[1].getMessage());
      }
   }

   public static class NonValidatingBuilder implements Builder<Object> {
      @Override
      public void validate() {
         throw new RuntimeException("MODULE ERROR");
      }

      @Override
      public Object create() {
         return null;
      }

      @Override
      public Builder<?> read(Object template) {
         return this;
      }
   }
}
