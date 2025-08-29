package org.infinispan.query.config;

import static org.testng.AssertJUnit.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.test.Person;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.config.CacheModeTest")
public class CacheModeTest extends AbstractInfinispanTest {
   private File tempDir;

   @BeforeMethod
   public void createTempDir() throws IOException {
      tempDir = Files.createTempDirectory(getClass().getName()).toFile();
   }

   @AfterMethod
   public void tearDown() {
      Util.recursiveFileRemove(tempDir);
   }

   public void testLocal() {
      doTest(CacheMode.LOCAL);
   }

   public void testReplicated() {
      doTest(CacheMode.REPL_SYNC);
   }

   @Test(expectedExceptions = CacheConfigurationException.class,
         expectedExceptionsMessageRegExp = "ISPN(\\d)*: Indexing can not be enabled on caches in Invalidation mode")
   public void testInvalidated() {
      doTest(CacheMode.INVALIDATION_SYNC);
   }

   public void testDistributed() {
      doTest(CacheMode.DIST_SYNC);
   }

   private void doTest(CacheMode m) {
      CacheContainer cc = null;
      try {
         ConfigurationBuilder builder = new ConfigurationBuilder();
         builder.clustering().cacheMode(m).indexing().enable().addIndexedEntities(Person.class);
         GlobalConfigurationBuilder globalBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
         globalBuilder.serialization().addContextInitializer(TestDataSCI.INSTANCE);
         globalBuilder.globalState().enable().persistentLocation(tempDir.getPath());
         cc = TestCacheManagerFactory.createClusteredCacheManager(globalBuilder, builder);
         QueryInterceptor queryInterceptor =
               TestingUtil.findInterceptor(cc.getCache(), QueryInterceptor.class);
         assertNotNull("Didn't find a query interceptor in the chain!!", queryInterceptor);
      } finally {
         TestingUtil.killCacheManagers(cc);
      }
   }
}
