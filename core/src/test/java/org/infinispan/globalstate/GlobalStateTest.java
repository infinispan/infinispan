package org.infinispan.globalstate;

import java.io.File;

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
      Util.recursiveFileRemove(stateDirectory);
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable().persistentLocation(stateDirectory);
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(false, global, new ConfigurationBuilder(), new TransportFlags(), false);
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(false, global, new ConfigurationBuilder(), new TransportFlags(), false);
      try {
         cm1.start();
         Exceptions.expectException(EmbeddedCacheManagerStartupException.class, "org.infinispan.commons.CacheConfigurationException: ISPN000512: Cannot acquire lock.*", () -> cm2.start());
      } finally {
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }
}
