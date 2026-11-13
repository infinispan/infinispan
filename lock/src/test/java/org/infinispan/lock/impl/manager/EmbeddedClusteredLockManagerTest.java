package org.infinispan.lock.impl.manager;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.test.TestingUtil.getListeners;
import static org.infinispan.testing.Exceptions.expectException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.infinispan.Cache;
import org.infinispan.lock.BaseClusteredLockTest;
import org.infinispan.lock.api.ClusteredLock;
import org.infinispan.lock.api.ClusteredLockConfiguration;
import org.infinispan.lock.api.ClusteredLockManager;
import org.infinispan.lock.api.OwnershipLevel;
import org.infinispan.lock.exception.ClusteredLockException;
import org.infinispan.lock.impl.ClusteredLockModuleLifecycle;
import org.infinispan.lock.impl.lock.ClusteredLockImpl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "clusteredLock.EmbeddedClusteredLockManagerTest")
public class EmbeddedClusteredLockManagerTest extends BaseClusteredLockTest {

   private static final String LOCK_NAME = "EmbeddedClusteredLockManagerTest";

   @AfterMethod
   public void after() {
      ClusteredLockManager clusteredLockManager = clusteredLockManager(0);
      await(clusteredLockManager.remove(LOCK_NAME));
   }

   public void testDefineLock() {
      ClusteredLockManager clusteredLockManager = clusteredLockManager(0);
      assertTrue(clusteredLockManager.defineLock(LOCK_NAME, new ClusteredLockConfiguration()));
      assertFalse(clusteredLockManager.defineLock(LOCK_NAME, new ClusteredLockConfiguration()));
   }

   public void testGetWithLockDefinition() {
      ClusteredLockManager clusteredLockManager = clusteredLockManager(0);
      clusteredLockManager.defineLock(LOCK_NAME, new ClusteredLockConfiguration());
      ClusteredLock lock = clusteredLockManager.get(LOCK_NAME);
      assertNotNull(lock);
   }

   public void testGetWithLockDefinitionFromAnotherNode() {
      ClusteredLockManager cm0 = clusteredLockManager(0);
      cm0.defineLock(LOCK_NAME, new ClusteredLockConfiguration());
      ClusteredLockImpl lock0 = (ClusteredLockImpl) cm0.get(LOCK_NAME);
      ClusteredLockManager cm1 = clusteredLockManager(1);
      ClusteredLockImpl lock1 = (ClusteredLockImpl) cm1.get(LOCK_NAME);

      assertNotNull(lock0);
      assertNotNull(lock1);

      assertEquals(lock0.getName(), lock1.getName());
      assertEquals(manager(0).getAddress(), lock0.getOriginator());
      assertEquals(manager(1).getAddress(), lock1.getOriginator());
   }

   public void testGetWithoutLockDefinition() {
      ClusteredLockManager clusteredLockManager = clusteredLockManager(0);
      expectException(ClusteredLockException.class, () -> clusteredLockManager.get(LOCK_NAME));
   }

   public void testGetConfigurationWithLockDefinition() {
      ClusteredLockManager clusteredLockManager = clusteredLockManager(0);
      clusteredLockManager.defineLock(LOCK_NAME, new ClusteredLockConfiguration());
      ClusteredLockConfiguration configuration = clusteredLockManager.getConfiguration(LOCK_NAME);
      assertEquals(OwnershipLevel.NODE, configuration.getOwnershipLevel());
      assertFalse(configuration.isReentrant());
   }

   public void testGetConfigurationWithoutLockDefinition() {
      ClusteredLockManager clusteredLockManager = clusteredLockManager(0);
      expectException(ClusteredLockException.class, () -> clusteredLockManager.getConfiguration(LOCK_NAME));
   }

   public void testIsDefined() {
      ClusteredLockManager clusteredLockManager = clusteredLockManager(0);
      assertFalse(clusteredLockManager.isDefined(LOCK_NAME));
      clusteredLockManager.defineLock(LOCK_NAME, new ClusteredLockConfiguration());
      assertTrue(clusteredLockManager.isDefined(LOCK_NAME));
   }

   public void testForceRelease() {
      ClusteredLockManager clusteredLockManager = clusteredLockManager(0);
      clusteredLockManager.defineLock(LOCK_NAME, new ClusteredLockConfiguration());
      ClusteredLock lock = clusteredLockManager.get(LOCK_NAME);
      await(lock.lock());
      assertTrue(await(lock.isLocked()));
      assertTrue(await(clusteredLockManager.forceRelease(LOCK_NAME)));
      assertFalse(await(lock.isLocked()));
   }

   public void testForceReleaseUndefinedLock() {
      ClusteredLockManager clusteredLockManager = clusteredLockManager(0);
      assertFalse(await(clusteredLockManager.forceRelease(LOCK_NAME)));
   }

   public void testRemove() {
      ClusteredLockManager clusteredLockManager = clusteredLockManager(0);
      clusteredLockManager.defineLock(LOCK_NAME);
      assertNotNull(clusteredLockManager.get(LOCK_NAME));
      assertTrue(await(clusteredLockManager.remove(LOCK_NAME)));
      assertFalse(clusteredLockManager.isDefined(LOCK_NAME));
   }

   /**
    * Locks must not install a cluster listener each, otherwise every lock creation and removal is broadcast to the
    * whole cluster. A single listener per node, filtered by the node id, is enough.
    */
   public void testSingleClusterListenerPerNode() {
      ClusteredLockManager clusteredLockManager = clusteredLockManager(0);
      Cache<Object, Object> clusteredLocksCache = cache(0, ClusteredLockModuleLifecycle.CLUSTERED_LOCK_CACHE_NAME);

      clusteredLockManager.defineLock(LOCK_NAME);
      clusteredLockManager.get(LOCK_NAME);
      int listenersWithOneLock = getListeners(clusteredLocksCache).size();

      for (int i = 0; i < 10; i++) {
         String name = LOCK_NAME + "-" + i;
         clusteredLockManager.defineLock(name);
         assertNotNull(clusteredLockManager.get(name));
      }
      assertEquals(listenersWithOneLock, getListeners(clusteredLocksCache).size());

      for (int i = 0; i < 10; i++) {
         await(clusteredLockManager.remove(LOCK_NAME + "-" + i));
      }
      assertEquals(listenersWithOneLock, getListeners(clusteredLocksCache).size());
   }
}
