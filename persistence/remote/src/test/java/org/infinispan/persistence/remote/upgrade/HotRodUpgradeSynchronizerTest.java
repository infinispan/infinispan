package org.infinispan.persistence.remote.upgrade;

import static org.infinispan.client.hotrod.ProtocolVersion.DEFAULT_PROTOCOL_VERSION;
import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.upgrade.RollingUpgradeManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(testName = "upgrade.hotrod.HotRodUpgradeSynchronizerTest", groups = "functional")
public class HotRodUpgradeSynchronizerTest extends AbstractInfinispanTest {

   private TestCluster sourceCluster, targetCluster;

   private static final String OLD_CACHE = "old-cache";
   private static final String TEST_CACHE = HotRodUpgradeSynchronizerTest.class.getName();

   private static final String OLD_PROTOCOL_VERSION = "2.0";
   private static final String NEW_PROTOCOL_VERSION = DEFAULT_PROTOCOL_VERSION.toString();

   @BeforeClass
   public void setup() throws Exception {
      sourceCluster = new TestCluster.Builder().setName("sourceCluster").setNumMembers(1)
            .cache().name(OLD_CACHE)
            .cache().name(TEST_CACHE)
            .build();

      targetCluster = new TestCluster.Builder().setName("targetCluster").setNumMembers(1)
            .cache().name(OLD_CACHE).remotePort(sourceCluster.getHotRodPort()).remoteProtocolVersion(OLD_PROTOCOL_VERSION)
            .cache().name(TEST_CACHE).remotePort(sourceCluster.getHotRodPort()).remoteProtocolVersion(NEW_PROTOCOL_VERSION)
            .build();
   }

   public void testSynchronizationViaIterator() throws Exception {
      // Fill the old cluster with data
      for (char ch = 'A'; ch <= 'Z'; ch++) {
         String s = Character.toString(ch);
         sourceCluster.getRemoteCache(TEST_CACHE).put(s, s, 20, TimeUnit.SECONDS, 30, TimeUnit.SECONDS);
      }
      // Verify access to some of the data from the new cluster
      assertEquals("A", targetCluster.getRemoteCache(TEST_CACHE).get("A"));

      RollingUpgradeManager targetUpgradeManager = targetCluster.getRollingUpgradeManager(TEST_CACHE);
      targetUpgradeManager.synchronizeData("hotrod");
      targetUpgradeManager.disconnectSource("hotrod");

      assertEquals(sourceCluster.getRemoteCache(TEST_CACHE).size(), targetCluster.getRemoteCache(TEST_CACHE).size());

      MetadataValue<String> metadataValue = targetCluster.getRemoteCache(TEST_CACHE).getWithMetadata("B");
      assertEquals(20, metadataValue.getLifespan());
      assertEquals(30, metadataValue.getMaxIdle());
   }

   public void testSynchronizationViaKeyRecording() throws Exception {
      // Fill the old cluster with data
      for (char ch = 'A'; ch <= 'Z'; ch++) {
         String s = Character.toString(ch);
         sourceCluster.getRemoteCache(OLD_CACHE).put(s, s, 20, TimeUnit.SECONDS, 30, TimeUnit.SECONDS);
      }
      // Verify access to some of the data from the new cluster
      assertEquals("A", targetCluster.getRemoteCache(OLD_CACHE).get("A"));

      sourceCluster.getRollingUpgradeManager(OLD_CACHE).recordKnownGlobalKeyset();

      RollingUpgradeManager targetUpgradeManager = targetCluster.getRollingUpgradeManager(OLD_CACHE);
      targetUpgradeManager.synchronizeData("hotrod");
      targetUpgradeManager.disconnectSource("hotrod");

      assertEquals(sourceCluster.getRemoteCache(OLD_CACHE).size() - 1, targetCluster.getRemoteCache(OLD_CACHE).size());

      MetadataValue<String> metadataValue = targetCluster.getRemoteCache(OLD_CACHE).getWithMetadata("A");
      assertEquals(20, metadataValue.getLifespan());
      assertEquals(30, metadataValue.getMaxIdle());
   }


   @BeforeMethod
   public void cleanup() {
      sourceCluster.cleanAllCaches();
      targetCluster.cleanAllCaches();
   }

   @AfterClass
   public void tearDown() {
      sourceCluster.destroy();
      targetCluster.destroy();
   }

}
