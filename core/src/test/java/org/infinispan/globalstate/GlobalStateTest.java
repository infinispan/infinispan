package org.infinispan.globalstate;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.lang.reflect.Method;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.EmbeddedCacheManagerStartupException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.Exceptions;
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
      String stateDirectory = TestingUtil.tmpDirectory(this.getClass().getSimpleName() + File.separator + "COMMON");

      GlobalConfigurationBuilder global1 = statefulGlobalBuilder(stateDirectory, true);
      GlobalConfigurationBuilder global2 = statefulGlobalBuilder(stateDirectory, true);

      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(false, global1, new ConfigurationBuilder(), new TransportFlags(), false);
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(false, global2, new ConfigurationBuilder(), new TransportFlags(), false);
      try {
         cm1.start();
         Exceptions.expectException(EmbeddedCacheManagerStartupException.class, "org.infinispan.commons.CacheConfigurationException: ISPN000512: Cannot acquire lock.*", () -> cm2.start());
      } finally {
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

   public void testCorruptGlobalState(Method m) throws Exception {
      String state1 = TestingUtil.tmpDirectory(this.getClass().getSimpleName() + File.separator + m.getName() + "1");
      GlobalConfigurationBuilder global1 = statefulGlobalBuilder(state1, true);
      String state2 = TestingUtil.tmpDirectory(this.getClass().getSimpleName() + File.separator + m.getName() + "2");
      GlobalConfigurationBuilder global2 = statefulGlobalBuilder(state2, true);
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(false, global1, new ConfigurationBuilder(), new TransportFlags(), false);
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(false, global2, new ConfigurationBuilder(), new TransportFlags(), false);
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
         EmbeddedCacheManager newCm1 = TestCacheManagerFactory.createClusteredCacheManager(false, global1, new ConfigurationBuilder(), new TransportFlags(), false);
         Exceptions.expectException(EmbeddedCacheManagerStartupException.class, "org.infinispan.commons.CacheConfigurationException: ISPN000516: The state file for '___global' is invalid.*", () -> newCm1.start());
      } finally {
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

   private GlobalConfigurationBuilder statefulGlobalBuilder(String stateDirectory, boolean clear) {
      if (clear) Util.recursiveFileRemove(stateDirectory);
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable().persistentLocation(stateDirectory);
      return global;
   }
}
