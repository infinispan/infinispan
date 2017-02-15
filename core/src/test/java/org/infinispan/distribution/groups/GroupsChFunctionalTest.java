package org.infinispan.distribution.groups;

import org.infinispan.Cache;
import org.infinispan.distribution.DistSyncFuncTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Pete Muir
 * @since 5.0
 */
@Test(groups = "functional", testName = "distribution.groups.GroupsChFunctionalTest")
@CleanupAfterMethod
public class GroupsChFunctionalTest extends DistSyncFuncTest {

   public GroupsChFunctionalTest() {
      groupers = true;
   }

   public void testGrouper() throws Throwable {
      for (Cache<Object, String> c : caches) assert c.isEmpty();

      // Based on the grouping fn which uses computes a group by taking the digit from kX
      // and doing a modulo 2 on it we can verify the owners of keys
      Assert.assertNotSame(getOwners("k1"), getOwners("k2"));
      Assert.assertNotSame(getOwners("k1"), getOwners("k4"));
      Assert.assertNotSame(getOwners("k3"), getOwners("k2"));
      Assert.assertNotSame(getOwners("k3"), getOwners("k4"));
      Assert.assertEquals(getOwners("k1"), getOwners("k3"));
      Assert.assertEquals(getOwners("k2"), getOwners("k4"));

   }

   public void testIntrinsicGrouping() throws Throwable {
      for (Cache<Object, String> c : caches) assert c.isEmpty();

      GroupedKey k1 = new GroupedKey("groupA", "k1");
      GroupedKey k2 = new GroupedKey("groupB", "k2");
      GroupedKey k3 = new GroupedKey("groupA", "k3");
      GroupedKey k4 = new GroupedKey("groupB", "k4");

      Assert.assertNotSame(getOwners(k1), getOwners(k2));
      Assert.assertNotSame(getOwners(k1), getOwners(k4));
      Assert.assertNotSame(getOwners(k3), getOwners(k2));
      Assert.assertNotSame(getOwners(k3), getOwners(k4));
      Assert.assertEquals(getOwners(k1), getOwners(k3));
      Assert.assertEquals(getOwners(k2), getOwners(k4));

      GroupedKey k1A = new GroupedKey("groupA", "k1");
      GroupedKey k1B = new GroupedKey("groupB", "k1");

      // Check that the same key in different groups is mapped to different nodes (nb this is not something you want to really do!)
      Assert.assertNotSame(getOwners(k1A), getOwners(k1B));

   }

   public void testRehash() throws Throwable {
      for (Cache<Object, String> c : caches) assert c.isEmpty();

      GroupedKey k1 = new GroupedKey("groupA", "k1");
      GroupedKey k2 = new GroupedKey("groupA", "k2");
      GroupedKey k3 = new GroupedKey("groupA", "k3");
      GroupedKey k4 = new GroupedKey("groupA", "k4");

      Assert.assertEquals(getOwners(k1), getOwners(k2));
      Assert.assertEquals(getOwners(k1), getOwners(k3));
      Assert.assertEquals(getOwners(k1), getOwners(k4));

      Cache<Object, String>[] owners1 = getOwners(k1);
      Cache<Object, String>[] owners2 = getOwners(k2);
      Cache<Object, String>[] owners3 = getOwners(k3);
      Cache<Object, String>[] owners4 = getOwners(k4);

      final Cache owner = getOwners("groupA")[0];
      int ownerIndex = -1;

      for (int i = 0; i < caches.size(); i++) {
         if (owner == caches.get(i)) {
            ownerIndex = i;
            break;
         }
      }

      assert ownerIndex != -1;

      TestingUtil.killCacheManagers(manager(ownerIndex));
      caches.remove(ownerIndex);
      cacheManagers.remove(ownerIndex);
      TestingUtil.waitForStableTopology(caches);

      Assert.assertNotSame(getOwners(k1), owners1);
      Assert.assertNotSame(getOwners(k2), owners2);
      Assert.assertNotSame(getOwners(k3), owners3);
      Assert.assertNotSame(getOwners(k4), owners4);

      Assert.assertEquals(getOwners(k1), getOwners(k2));
      Assert.assertEquals(getOwners(k1), getOwners(k3));
      Assert.assertEquals(getOwners(k1), getOwners(k4));

   }
}
