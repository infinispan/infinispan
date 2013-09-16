package org.infinispan.configuration;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import java.util.Properties;

/**
 * /**
 * Tests that string property replacement works properly when parsing
 * a config file.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test (groups = "functional", testName = "config.StringPropertyReplacementTest")
public class StringPropertyReplacementTest extends SingleCacheManagerTest {


   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      System.setProperty("test.property.asyncListenerMaxThreads","2");
      System.setProperty("test.property.persistenceMaxThreads","2");
      System.setProperty("test.property.IsolationLevel","READ_COMMITTED");
      System.setProperty("test.property.writeSkewCheck","true");
      System.setProperty("test.property.SyncCommitPhase","true");
      return TestCacheManagerFactory.fromXml("configs/string-property-replaced.xml");
   }

   public void testGlobalConfig() {
      Properties asyncListenerExecutorProperties = cacheManager.getCacheManagerConfiguration().asyncListenerExecutor().properties();
      asyncListenerExecutorProperties.get("maxThreads").equals("2");

      Properties persistenceExecutorProperties = cacheManager.getCacheManagerConfiguration().persistenceExecutor().properties();
      persistenceExecutorProperties.get("maxThreads").equals("4");

      Properties transportProps = cacheManager.getCacheManagerConfiguration().transport().properties();
      // Should be "jgroups-tcp.xml", but gets overriden by test cache manager factory
      assert transportProps.get("configurationFile") == null;
   }

   public void testDefaultCache() {
      org.infinispan.configuration.cache.Configuration configuration = cacheManager.getCache().getCacheConfiguration();
      assert configuration.locking().isolationLevel().equals(IsolationLevel.READ_COMMITTED);
      assert !configuration.locking().writeSkewCheck();
      assert configuration.transaction().syncCommitPhase();
   }
}

