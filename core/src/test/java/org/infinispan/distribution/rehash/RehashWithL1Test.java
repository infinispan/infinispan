package org.infinispan.distribution.rehash;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoName;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;
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
@Test(groups = {"functional", "unstable"}, testName = "distribution.rehash.RehashWithL1Test", description = "See ISPN-7801")
public class RehashWithL1Test extends MultipleCacheManagersTest {

   ConfigurationBuilder builder;

   @Override
   protected void createCacheManagers() throws Throwable {
      MyBaseControlledConsistentHashFactory chf = new MyBaseControlledConsistentHashFactory();
      builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numSegments(1).numOwners(1).consistentHashFactory(chf);
      builder.clustering().l1().enable().lifespan(10, TimeUnit.MINUTES);
      createClusteredCaches(3, RehashWithL1TestSCI.INSTANCE, builder);
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

   @ProtoName("MyBaseControlledConsistentHashFactory")
   public static class MyBaseControlledConsistentHashFactory extends BaseControlledConsistentHashFactory<DefaultConsistentHash> {
      public MyBaseControlledConsistentHashFactory() {
         super(new DefaultTrait(), 1);
      }

      @Override
      protected int[][] assignOwners(int numSegments, List<Address> members) {
         return new int[][]{{members.size() - 1}};
      }
   }

   @ProtoSchema(
         includeClasses = RehashWithL1Test.MyBaseControlledConsistentHashFactory.class,
         schemaFileName = "test.core.RehashWithL1Test.proto",
         schemaFilePath = "proto/generated",
         schemaPackageName = "org.infinispan.test.core.RehashWithL1Test",
         service = false,
         syntax = ProtoSyntax.PROTO3
   )
   interface RehashWithL1TestSCI extends SerializationContextInitializer {
      SerializationContextInitializer INSTANCE = new RehashWithL1TestSCIImpl();
   }
}
