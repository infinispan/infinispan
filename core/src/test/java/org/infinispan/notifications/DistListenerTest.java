package org.infinispan.notifications;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.LookupMode;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * Used to verify which nodes are going to receive events in case it's configured
 * as DIST: all key owners and the node which is performing the operation will receive
 * a notification.
 * 
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
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

      List<Address> owners = cache(0).getAdvancedCache().getDistributionManager().locate(key1, LookupMode.READ);

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
      // TODO: should originators raise these events?  it seems broken
      assertCreated(true);
      
      //listen on non-owner non-putting:
      owner1.put(key1, "hello");
      assertModified(false);
      assertCreated(false);
   }
   
   private void assertCreated(boolean b) {
      assert listener.created == b;
      listener.created = false;
   }
   
   private void assertModified(boolean b) {
      assert listener.modified == b;
      listener.modified = false;
   }

   private <K, V> Cache<K, V> getCacheForAddress(Address a) {
      for (Cache<K, V> c: this.<K, V>caches())
         if (c.getAdvancedCache().getRpcManager().getAddress().equals(a))
            return c;
      return null;
   }
   
   @Listener
   static public class TestListener {
      
      boolean created = false;
      boolean modified = false;
      
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
   }

}
