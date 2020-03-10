package org.infinispan.lock.jmx;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.test.fwk.TestCacheManagerFactory.configureJmx;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Optional;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.infinispan.Cache;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.lock.BaseClusteredLockTest;
import org.infinispan.lock.api.ClusteredLock;
import org.infinispan.lock.api.ClusteredLockManager;
import org.infinispan.lock.impl.ClusteredLockModuleLifecycle;
import org.infinispan.lock.impl.manager.EmbeddedClusteredLockManager;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * JMX operations tests.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@Test(groups = "functional", testName = "clusteredLock.jmx.ClusteredLockJmxTest")
public class ClusteredLockJmxTest extends BaseClusteredLockTest {

   private static final String LOCK_NAME = ClusteredLockJmxTest.class.getSimpleName();

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   public void testForceRelease() {
      ClusteredLockManager clm = clusteredLockManager(0);
      assertTrue(clusteredLockManager(0).defineLock(LOCK_NAME));
      ClusteredLock lock = clm.get(LOCK_NAME);
      assertTrue(await(lock.tryLock()));
      assertTrue(await(lock.isLocked()));

      assertTrue(executeClusteredLockNameArgOperation(0, EmbeddedClusteredLockManager.FORCE_RELEASE, LOCK_NAME));
      assertFalse(await(lock.isLocked()));
   }

   public void testRemove() {
      ClusteredLockManager clm = clusteredLockManager(0);
      assertTrue(clm.defineLock(LOCK_NAME));
      assertFalse(clm.defineLock(LOCK_NAME));

      // first call remove result should be true because the lock exists
      assertTrue(executeClusteredLockNameArgOperation(0, EmbeddedClusteredLockManager.REMOVE, LOCK_NAME));
      // second call remove result should be false
      assertFalse(executeClusteredLockNameArgOperation(0, EmbeddedClusteredLockManager.REMOVE, LOCK_NAME));

      assertTrue(clm.defineLock(LOCK_NAME));
   }

   public void testIsDefined() {
      assertFalse(executeClusteredLockNameArgOperation(0, EmbeddedClusteredLockManager.IS_DEFINED, LOCK_NAME));

      assertTrue(clusteredLockManager(0).defineLock(LOCK_NAME));

      assertTrue(executeClusteredLockNameArgOperation(0, EmbeddedClusteredLockManager.IS_DEFINED, LOCK_NAME));
   }

   public void testIsLocked() {
      assertFalse(executeClusteredLockNameArgOperation(0, EmbeddedClusteredLockManager.IS_LOCKED, LOCK_NAME));

      ClusteredLockManager clm = clusteredLockManager(0);
      assertTrue(clm.defineLock(LOCK_NAME));
      assertFalse(executeClusteredLockNameArgOperation(0, EmbeddedClusteredLockManager.IS_LOCKED, LOCK_NAME));

      ClusteredLock lock = clm.get(LOCK_NAME);
      assertTrue(await(lock.tryLock()));
      assertTrue(await(lock.isLocked()));

      assertTrue(executeClusteredLockNameArgOperation(0, EmbeddedClusteredLockManager.IS_LOCKED, LOCK_NAME));
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() throws Throwable {
      super.clearContent();
      findCache(ClusteredLockModuleLifecycle.CLUSTERED_LOCK_CACHE_NAME).ifPresent(Cache::clear);
   }

   @Override
   protected int clusterSize() {
      return 2;
   }

   @Override
   protected GlobalConfigurationBuilder configure(int nodeId) {
      GlobalConfigurationBuilder builder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      String jmxDomain = getClass().getSimpleName() + nodeId;
      configureJmx(builder, jmxDomain, mBeanServerLookup);
      return builder;
   }

   private Optional<Cache<?, ?>> findCache(String cacheName) {
      return Optional.ofNullable(manager(0).getCache(cacheName, false));
   }

   private <T> T executeClusteredLockNameArgOperation(int index, String operationName, String arg) {
      MBeanServer server = mBeanServerLookup.getMBeanServer();
      try {
         //noinspection unchecked
         return (T) server
               .invoke(clusteredLockObjectName(index), operationName, new Object[]{arg},
                     new String[]{String.class.getName()});
      } catch (InstanceNotFoundException | MBeanException | ReflectionException e) {
         throw new RuntimeException(e);
      }
   }

   private ObjectName clusteredLockObjectName(int managerIndex) {
      final String domain = manager(managerIndex).getCacheManagerConfiguration().jmx().domain();
      return TestingUtil.getCacheManagerObjectName(domain, "DefaultCacheManager", EmbeddedClusteredLockManager.OBJECT_NAME);
   }
}
