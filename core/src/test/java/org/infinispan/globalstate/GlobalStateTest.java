package org.infinispan.globalstate;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.infinispan.commons.test.Exceptions.expectException;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.lang.reflect.Method;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.EmbeddedCacheManagerStartupException;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

/**
 * @since 9.2
 */
@Test(testName = "globalstate.GlobalStateTest", groups = "functional")
public class GlobalStateTest extends AbstractInfinispanTest {

   public void testLockPersistentLocation() {
      String stateDirectory = tmpDirectory(this.getClass().getSimpleName(), "COMMON");

      GlobalConfigurationBuilder global1 = statefulGlobalBuilder(stateDirectory, true);
      GlobalConfigurationBuilder global2 = statefulGlobalBuilder(stateDirectory, true);

      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(false, global1, new ConfigurationBuilder(), new TransportFlags());
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(false, global2, new ConfigurationBuilder(), new TransportFlags());
      try {
         cm1.start();
         expectException(EmbeddedCacheManagerStartupException.class, "org.infinispan.commons.CacheConfigurationException: ISPN000512: Cannot acquire lock.*", () -> cm2.start());
      } finally {
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

   public void testCorruptGlobalState(Method m) throws Exception {
      String state1 = tmpDirectory(this.getClass().getSimpleName(), m.getName() + "1");
      GlobalConfigurationBuilder global1 = statefulGlobalBuilder(state1, true);
      String state2 = tmpDirectory(this.getClass().getSimpleName(), m.getName() + "2");
      GlobalConfigurationBuilder global2 = statefulGlobalBuilder(state2, true);
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(false, global1, new ConfigurationBuilder(), new TransportFlags());
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(false, global2, new ConfigurationBuilder(), new TransportFlags());
      try {
         cm1.start();
         cm2.start();
         cm1.stop();
         cm2.stop();
         // corrupt one of the state files
         Writer w = new FileWriter(new File(state1, ScopedPersistentState.GLOBAL_SCOPE + ".state"));
         w.write("'Cause I want to be anarchy\nIt's the only way to be");
         w.close();
         global1 = statefulGlobalBuilder(state1, false);
         EmbeddedCacheManager newCm1 = TestCacheManagerFactory.createClusteredCacheManager(false, global1, new ConfigurationBuilder(), new TransportFlags());
         expectException(EmbeddedCacheManagerStartupException.class, "org.infinispan.commons.CacheConfigurationException: ISPN000516: The state file for '___global' is invalid.*", () -> newCm1.start());
      } finally {
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

   public void testIncompatibleGlobalState(Method m) throws Exception {
      String state1 = tmpDirectory(this.getClass().getSimpleName(), m.getName() + "1");
      GlobalConfigurationBuilder global1 = statefulGlobalBuilder(state1, true);
      String state2 = tmpDirectory(this.getClass().getSimpleName(), m.getName() + "2");
      GlobalConfigurationBuilder global2 = statefulGlobalBuilder(state2, true);
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(false, global1, new ConfigurationBuilder(), new TransportFlags());
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(false, global2, new ConfigurationBuilder(), new TransportFlags());
      try {
         cm1.start();
         cm2.start();
         // Create two DIST caches
         ConfigurationBuilder distBuilder = new ConfigurationBuilder();
         distBuilder.clustering().cacheMode(CacheMode.DIST_SYNC);
         cm1.administration().createCache("cache1", distBuilder.build());
         cm1.administration().createCache("cache2", distBuilder.build());
         cm2.stop();

         // Remove the caches from the first node
         cm1.administration().removeCache("cache1");
         cm1.administration().removeCache("cache2");
         // Recreate the cache as REPL
         ConfigurationBuilder replBuilder = new ConfigurationBuilder();
         replBuilder.clustering().cacheMode(CacheMode.REPL_SYNC);
         cm1.administration().createCache("cache1", replBuilder.build());

         // Attempt to restart the second cache manager
         global2 = statefulGlobalBuilder(state2, false);
         EmbeddedCacheManager newCm2 = TestCacheManagerFactory.createClusteredCacheManager(false, global2, new ConfigurationBuilder(), new TransportFlags());
         expectException(EmbeddedCacheManagerStartupException.class,
                         "(?s)org.infinispan.commons.CacheConfigurationException: ISPN000500: Cannot create clustered configuration for cache.*",
                         () -> newCm2.start());
      } finally {
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

   private GlobalConfigurationBuilder statefulGlobalBuilder(String stateDirectory, boolean clear) {
      if (clear) Util.recursiveFileRemove(stateDirectory);
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable().persistentLocation(stateDirectory).sharedPersistentLocation(stateDirectory).configurationStorage(ConfigurationStorage.OVERLAY);
      return global;
   }

   public void testFailStartup(Method m) throws Exception {
      String state = tmpDirectory(this.getClass().getSimpleName(), m.getName());
      GlobalConfigurationBuilder global = statefulGlobalBuilder(state, true);
      global.transport().transport(new FailingJGroupsTransport());
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(false, global, new ConfigurationBuilder(), new TransportFlags());
      try {
         cm.start();
         fail("Should not reach here");
      } catch (Exception e) {
         // Ensure there is no global state file
         File globalStateFile = new File(state, ScopedPersistentState.GLOBAL_SCOPE + ".state");
         assertFalse(globalStateFile.exists());
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public static class FailingJGroupsTransport extends JGroupsTransport {

      @Override
      public void start() {
         throw new RuntimeException("fail");
      }
   }
}
