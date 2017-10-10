package org.infinispan.lock.impl.manager;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.test.Exceptions.expectException;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.lock.BaseClusteredLockTest;
import org.infinispan.lock.api.ClusteredLock;
import org.infinispan.lock.api.ClusteredLockConfiguration;
import org.infinispan.lock.api.ClusteredLockManager;
import org.infinispan.lock.api.OwnershipLevel;
import org.infinispan.lock.exception.ClusteredLockException;
import org.infinispan.lock.impl.lock.ClusteredLockImpl;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "clusteredLock.EmbeddedClusteredLockManagerTest")
public class EmbeddedClusteredLockManagerTest extends BaseClusteredLockTest {

   public void testDefinelock() {
      ClusteredLockManager clusteredLockManager = clusteredLockManager(0);
      assertTrue(clusteredLockManager.defineLock("lock", new ClusteredLockConfiguration()));
      assertFalse(clusteredLockManager.defineLock("lock", new ClusteredLockConfiguration()));
   }

   public void testGetWithLockDefinition() {
      ClusteredLockManager clusteredLockManager = clusteredLockManager(0);
      clusteredLockManager.defineLock("superLock", new ClusteredLockConfiguration());
      ClusteredLock lock = clusteredLockManager.get("superLock");
      assertNotNull(lock);
   }

   public void testGetWithLockDefinitionFromAnotherNode() {
      ClusteredLockManager cm0 = clusteredLockManager(0);
      cm0.defineLock("lockFromC0", new ClusteredLockConfiguration());
      ClusteredLockImpl lock0 = (ClusteredLockImpl) cm0.get("lockFromC0");
      ClusteredLockManager cm1 = clusteredLockManager(1);
      ClusteredLockImpl lock1 = (ClusteredLockImpl) cm1.get("lockFromC0");

      assertNotNull(lock0);
      assertNotNull(lock1);

      assertEquals(lock0.getName(), lock1.getName());
      assertEquals(manager(0).getAddress(), lock0.getOriginator());
      assertEquals(manager(1).getAddress(), lock1.getOriginator());
   }

   public void testGetWithoutLockDefinition() {
      ClusteredLockManager clusteredLockManager = clusteredLockManager(0);
      expectException(ClusteredLockException.class, () -> clusteredLockManager.get("notDefined"));
   }

   public void testGetConfigurationWithLockDefinition() {
      ClusteredLockManager clusteredLockManager = clusteredLockManager(0);
      clusteredLockManager.defineLock("superLock", new ClusteredLockConfiguration());
      ClusteredLockConfiguration configuration = clusteredLockManager.getConfiguration("superLock");
      assertEquals(OwnershipLevel.NODE, configuration.getOwnershipLevel());
      assertFalse(configuration.isReentrant());
   }

   public void testGetConfigurationWithoutLockDefinition() {
      ClusteredLockManager clusteredLockManager = clusteredLockManager(0);
      expectException(ClusteredLockException.class, () -> clusteredLockManager.getConfiguration("notDefined"));
   }

   public void testIsDefined() {
      ClusteredLockManager clusteredLockManager = clusteredLockManager(0);
      assertFalse(clusteredLockManager.isDefined("notDefined"));
      clusteredLockManager.defineLock("defineLock", new ClusteredLockConfiguration());
      assertTrue(clusteredLockManager.isDefined("defineLock"));
   }

   public void testForceRelease() {
      ClusteredLockManager clusteredLockManager = clusteredLockManager(0);
      clusteredLockManager.defineLock("defineLock", new ClusteredLockConfiguration());
      ClusteredLock lock = clusteredLockManager.get("defineLock");
      await(lock.lock());
      assertTrue(await(lock.isLocked()));
      assertTrue(await(clusteredLockManager.forceRelease("defineLock")));
      assertFalse(await(lock.isLocked()));
   }

   public void testForceReleaseUndefinedLock() {
      ClusteredLockManager clusteredLockManager = clusteredLockManager(0);
      assertFalse(await(clusteredLockManager.forceRelease("notDefined")));
   }
}
