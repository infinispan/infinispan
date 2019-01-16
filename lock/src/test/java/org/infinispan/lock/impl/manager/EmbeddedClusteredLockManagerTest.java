package org.infinispan.lock.impl.manager;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.test.Exceptions.expectException;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

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
      Cache<Object, Object> clusteredLocksCache = cache(0, ClusteredLockModuleLifecycle.CLUSTERED_LOCK_CACHE_NAME);
      int beforeSize = clusteredLocksCache.getListeners().size();
      clusteredLockManager.defineLock(LOCK_NAME);
      clusteredLockManager.get(LOCK_NAME);
      int afterSize = clusteredLocksCache.getListeners().size();
      assertTrue(beforeSize < afterSize);
      await(clusteredLockManager.remove(LOCK_NAME));
      int afterRemoveSize = clusteredLocksCache.getListeners().size();
      assertFalse(clusteredLockManager.isDefined(LOCK_NAME));
      assertTrue(beforeSize == afterRemoveSize);
   }
}
