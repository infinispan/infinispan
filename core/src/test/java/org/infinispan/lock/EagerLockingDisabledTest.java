package org.infinispan.lock;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.config.ConfigurationException;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tester for https://jira.jboss.org/browse/ISPN-596
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test (groups = "functional", testName = "lock.EagerLockingDisabledTest")
public class EagerLockingDisabledTest {

   public void testImplicitEagerLockingAndDld() {
      Configuration configuration = TestCacheManagerFactory.getDefaultConfiguration(false);
      configuration.setEnableDeadlockDetection(true);
      configuration.setUseEagerLocking(true);
      EmbeddedCacheManager cm = null;
      try {
         cm = TestCacheManagerFactory.createCacheManager(configuration);
         assert false : "expecting configuration exception";
      } catch (ConfigurationException ce) {
         //expected
      }
      finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testExplicitEagerLockingAndDld() {
      Configuration configuration = TestCacheManagerFactory.getDefaultConfiguration(false);
      configuration.setEnableDeadlockDetection(true);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(configuration);
      Cache c = cm.getCache();
      try {
         c.getAdvancedCache().lock("k");
      } catch (ConfigurationException ce) {
         //expected
      }
      finally {
         TestingUtil.killCacheManagers(cm);
      }
   }
}
