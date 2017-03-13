package org.infinispan.persistence.remote.upgrade;

import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.upgrade.RollingUpgradeManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(testName = "upgrade.hotrod.HotRodMultiServerUpdateSynchronizerTest", groups = "functional")
public class HotRodMultiServerUpdateSynchronizerTest extends AbstractInfinispanTest {

   private TestCluster sourceCluster, targetCluster;
   private static final String CACHE_NAME = HotRodMultiServerUpdateSynchronizerTest.class.getName();

   @BeforeClass
   public void setup() throws Exception {
      sourceCluster = new TestCluster.Builder().setName("sourceCluster").setNumMembers(2)
            .cache().name(CACHE_NAME)
            .build();
      targetCluster = new TestCluster.Builder().setName("targetCluster").setNumMembers(2)
            .cache().name(CACHE_NAME).remotePort(sourceCluster.getHotRodPort())
            .build();
   }

   public void testSynchronization() throws Exception {
      RemoteCache<String, String> sourceRemoteCache = sourceCluster.getRemoteCache(CACHE_NAME);
      RemoteCache<String, String> targetRemoteCache = targetCluster.getRemoteCache(CACHE_NAME);

      for (char ch = 'A'; ch <= 'Z'; ch++) {
         String s = Character.toString(ch);
         sourceRemoteCache.put(s, s, 20, TimeUnit.SECONDS, 30, TimeUnit.SECONDS);
      }

      // Verify access to some of the data from the new cluster
      assertEquals("A", targetRemoteCache.get("A"));

      RollingUpgradeManager upgradeManager = targetCluster.getRollingUpgradeManager(CACHE_NAME);
      long count = upgradeManager.synchronizeData("hotrod");

      assertEquals(26, count);
      assertEquals(sourceCluster.getEmbeddedCache(CACHE_NAME).size(), targetCluster.getEmbeddedCache(CACHE_NAME).size());

      upgradeManager.disconnectSource("hotrod");

      MetadataValue<String> metadataValue = targetRemoteCache.getWithMetadata("Z");
      assertEquals(20, metadataValue.getLifespan());
      assertEquals(30, metadataValue.getMaxIdle());

   }

   @AfterClass
   public void tearDown() {
      targetCluster.destroy();
      sourceCluster.destroy();
   }

}
