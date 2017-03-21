package org.infinispan.persistence.remote.upgrade;

import static java.util.stream.IntStream.range;
import static org.testng.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.upgrade.RollingUpgradeManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(testName = "upgrade.hotrod.HotRodUpgradeStressTest", groups = "stress")
public class HotRodUpgradeStressTest extends AbstractInfinispanTest {

   private static final String CACHE_NAME = "stress";

   private TestCluster sourceCluster, targetCluster;

   @BeforeClass
   public void setup() throws Exception {
      sourceCluster = new TestCluster.Builder().setName("sourceCluster").setNumMembers(2)
            .cache().name(CACHE_NAME)
            .build();

      targetCluster = new TestCluster.Builder().setName("targetCluster").setNumMembers(2)
            .cache().name(CACHE_NAME).remotePort(sourceCluster.getHotRodPort())
            .build();
   }

   @AfterClass
   public void tearDown() {
      targetCluster.destroy();
      sourceCluster.destroy();
   }


   void loadSourceCluster(int entries) {
      RemoteCache<String, String> remoteCache = sourceCluster.getRemoteCache(CACHE_NAME);
      AtomicInteger count = new AtomicInteger(0);
      range(0, entries).boxed().parallel().map(String::valueOf).forEach(k -> {
         remoteCache.put(k, "value" + k);
         int progress = count.incrementAndGet();
         if (progress % 10_000 == 0) {
            System.out.printf("Loaded %d\n", progress);
         }

      });
   }

   @Test
   public void testMigrate() throws Exception {
      loadSourceCluster(1_000_000);

      long start = System.currentTimeMillis();
      RollingUpgradeManager rum = targetCluster.getRollingUpgradeManager(CACHE_NAME);
      rum.synchronizeData("hotrod", 1000, 5);
      System.out.println("Elapsed (s): " + (System.currentTimeMillis() - start) / 1000);

      rum.disconnectSource("hotrod");

      assertEquals(targetCluster.getRemoteCache(CACHE_NAME).size(), sourceCluster.getRemoteCache(CACHE_NAME).size());

   }


}
