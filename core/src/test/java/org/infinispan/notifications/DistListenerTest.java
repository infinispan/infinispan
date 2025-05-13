package org.infinispan.notifications;

import static org.testng.AssertJUnit.assertEquals;

import java.util.List;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * Used to verify which nodes are going to receive events in case it's configured
 * as DIST: all key owners and the node which is performing the operation will receive
 * a notification.
 *
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 * @since 5.0
 */
@Test(groups = "functional", testName = "notifications.DistListenerTest")
public class DistListenerTest extends MultipleCacheManagersTest {

   private TestListener listener;

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(getDefaultClusteredCacheConfig(
            CacheMode.DIST_SYNC, true), 3);
      waitForClusterToForm();
   }

   public void testRemoteGet() {
      final String key1 = this.getClass().getName() + "K1";

      List<Address> owners = cacheTopology(0).getDistribution(key1).writeOwners();

      assert owners.size() == 2: "Key should have 2 owners";

      Cache<String, String> owner1 = getCacheForAddress(owners.get(0));
      Cache<String, String> owner2 = getCacheForAddress(owners.get(1));
      assert owner1 != owner2;
      Cache<String, String> nonOwner = null;
      for (int i=0; i<3; i++) {
         if (this.<String, String>cache(i) != owner1
               && this.<String, String>cache(i) != owner2) {
            nonOwner = cache(i);
            break;
         }
      }
      assert nonOwner != null;

      listener = new TestListener();

      // test owner puts and listens:
      assertCreated(false);
      assertModified(false);
      owner1.addListener(listener);
      owner1.put(key1, "hello");
      assertModified(false);
      assertCreated(true);
      assertCreated(false);
      assertModified(false);
      owner1.put(key1, "hello");
      assertModified(true);
      assertCreated(false);

      // now de-register:
      owner1.removeListener(listener);
      owner1.put(key1, "hello");
      assertModified(false);
      assertCreated(false);

      // put on non-owner and listens on owner:
      owner1.addListener(listener);
      nonOwner.put(key1, "hello");
      assertModified(true);
      assertCreated(false);
      owner1.removeListener(listener);
      assertModified(false);
      assertCreated(false);

      //listen on non-owner:
      nonOwner.addListener(listener);
      nonOwner.put(key1, "hello");
      assertModified(false);
      assertCreated(false);

      //listen on non-owner non-putting:
      owner1.put(key1, "hello");
      assertModified(false);
      assertCreated(false);
   }

   public void testRehashNoEvent() {
      listener = new TestListener();
      caches().forEach(c -> {
         c.addListener(listener);
         c.getCacheManager().addListener(listener);
      });
      // Insert some values
      IntStream.range(0, 10).boxed().forEach(i -> cache(0).put(i, i));
      assertCreated(true);
      assertModified(false);
      assertViewChanged(false);

      // Now add a new node and shutdown
      createClusteredCaches(1, getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true));
      killMember(cacheManagers.size() - 1);

      // We shouldn't have any events still...
      assertCreated(false);
      assertModified(false);
      assertViewChanged(true);
   }

   private void assertCreated(boolean b) {
      assertEquals(b, listener.created);
      listener.created = false;
   }

   private void assertModified(boolean b) {
      assertEquals(b, listener.modified);
      listener.modified = false;
   }

   private void assertViewChanged(boolean b) {
      assertEquals(b, listener.viewChanged);
      listener.viewChanged = false;
   }

   private <K, V> Cache<K, V> getCacheForAddress(Address a) {
      for (Cache<K, V> c: this.<K, V>caches())
         if (c.getAdvancedCache().getRpcManager().getAddress().equals(a))
            return c;
      return null;
   }

   @Listener
   public static class TestListener {

      boolean created = false;
      boolean modified = false;
      boolean viewChanged = false;

      @CacheEntryCreated
      @SuppressWarnings("unused")
      public void create(CacheEntryEvent e) {
         created = true;
      }

      @CacheEntryModified
      @SuppressWarnings("unused")
      public void modify(CacheEntryEvent e) {
         modified = true;
      }

      @ViewChanged
      @SuppressWarnings("unused")
      public void viewChanged(ViewChangedEvent e) {
         viewChanged = true;
      }
   }

}
