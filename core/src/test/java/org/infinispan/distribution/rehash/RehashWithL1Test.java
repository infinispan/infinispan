package org.infinispan.distribution.rehash;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.util.BaseControlledConsistentHashFactory;
import org.testng.annotations.Test;

/**
 * Tests rehashing with distributed caches with L1 enabled.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Test(groups = "functional", testName = "distribution.rehash.RehashWithL1Test")
public class RehashWithL1Test extends MultipleCacheManagersTest {

   ConfigurationBuilder builder;

   @Override
   protected void createCacheManagers() throws Throwable {
      MyBaseControlledConsistentHashFactory chf = new MyBaseControlledConsistentHashFactory();
      builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numSegments(1).numOwners(1).consistentHashFactory(chf);
      builder.clustering().l1().enable().lifespan(10, TimeUnit.MINUTES);
      createClusteredCaches(3, builder);
   }

   public void testPutWithRehashAndCacheClear() throws Exception {
      int opCount = 10;
      for (int i = 0; i < opCount; i++) {
         cache(1).put("k" + i, "some data");
      }

      for (int j = 0; j < caches().size(); j++) {
         log.debugf("Populating L1 on %s", address(j));
         for (int i = 0; i < opCount; i++) {
            assertEquals("Wrong value for k" + i, "some data", cache(j).get("k" + i));
         }
      }

      int killIndex = caches().size() - 1;
      log.debugf("Killing node %s", address(killIndex));
      killMember(killIndex);

      // All entries were owned by the killed node, but they survive in the L1 of cache(1)
      for (int j = 0; j < caches().size(); j++) {
         log.debugf("Checking values on %s", address(j));
         for (int i = 0; i < opCount; i++) {
            String key = "k" + i;
            assertEquals("Wrong value for key " + key, "some data", cache(j).get(key));
         }
      }

      log.debugf("Starting a new joiner");
      EmbeddedCacheManager cm = addClusterEnabledCacheManager(builder);
      cm.getCache();

      // State transfer won't copy L1 entries to cache(2), and they're deleted on cache(1) afterwards
      // Note: we would need eventually() if we checked the data container directly
      for (int j = 0; j < caches().size() - 1; j++) {
         log.debugf("Checking values on %s", address(j));
         for (int i = 0; i < opCount; i++) {
            assertNull("wrong value for k" + i, cache(j).get("k" + i));
         }
      }

      for (int i = 0; i < opCount; i++) {
         cache(0).remove("k" + i);
      }

      for (int i = 0; i < opCount; i++) {
         String key = "k" + i;
         assertFalse(cache(0).containsKey(key));
         assertFalse("Key: " + key + " is present in cache at " + cache(0),
                     cache(0).containsKey(key));
         assertFalse("Key: " + key + " is present in cache at " + cache(1),
               cache(1).containsKey(key));
         assertFalse("Key: " + key + " is present in cache at " + cache(2),
               cache(2).containsKey(key));
      }

      assertEquals(0, cache(0).size());
      assertEquals(0, cache(1).size());
      assertEquals(0, cache(2).size());
   }

   @SerializeWith(MyBaseControlledConsistentHashFactory.Ext.class)
   private static class MyBaseControlledConsistentHashFactory extends BaseControlledConsistentHashFactory<DefaultConsistentHash> {
      public MyBaseControlledConsistentHashFactory() {
         super(new DefaultTrait(), 1);
      }

      @Override
      protected List<Address> createOwnersCollection(List<Address> members, int numberOfOwners, int segmentIndex) {
         return Collections.singletonList(members.get(members.size() - 1));
      }

      public static final class Ext implements Externalizer<MyBaseControlledConsistentHashFactory> {
         @Override
         public void writeObject(ObjectOutput output, MyBaseControlledConsistentHashFactory object) {
            // No-op
         }

         @Override
         public MyBaseControlledConsistentHashFactory readObject(ObjectInput input) {
            return new MyBaseControlledConsistentHashFactory();
         }
      }
   }
}
