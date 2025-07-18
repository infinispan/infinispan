package org.infinispan.persistence.remote.upgrade;

import static org.infinispan.client.hotrod.ProtocolVersion.PROTOCOL_VERSION_31;
import static org.infinispan.client.hotrod.ProtocolVersion.PROTOCOL_VERSION_40;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.remote.RemoteStore;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.upgrade.RollingUpgradeManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Test for Hot Rod Rolling Upgrades using different storage types in caches
 *
 * @since 12.0
 */
@Test(testName = "upgrade.hotrod.HotRodUpgradeMediaTypesTest", groups = "functional")
public class HotRodUpgradeMediaTypesTest extends AbstractInfinispanTest {
   protected TestCluster sourceCluster, targetCluster;

   protected static final String CACHE_NAME = "theCache";

   public static final int ENTRIES = 10;
   private MediaType mediaType;
   private ProtocolVersion version;

   @Factory
   public Object[] factory() {
      return new Object[]{
            new HotRodUpgradeMediaTypesTest().protocolVersion(PROTOCOL_VERSION_31),
            new HotRodUpgradeMediaTypesTest().protocolVersion(PROTOCOL_VERSION_40),
            new HotRodUpgradeMediaTypesTest().mediaType(MediaType.TEXT_PLAIN).protocolVersion(PROTOCOL_VERSION_31),
            new HotRodUpgradeMediaTypesTest().mediaType(MediaType.TEXT_PLAIN).protocolVersion(PROTOCOL_VERSION_40),
            new HotRodUpgradeMediaTypesTest().mediaType(MediaType.APPLICATION_PROTOSTREAM).protocolVersion(PROTOCOL_VERSION_31),
            new HotRodUpgradeMediaTypesTest().mediaType(MediaType.APPLICATION_PROTOSTREAM).protocolVersion(PROTOCOL_VERSION_40),
            new HotRodUpgradeMediaTypesTest().mediaType(MediaType.APPLICATION_SERIALIZED_OBJECT).protocolVersion(PROTOCOL_VERSION_31),
            new HotRodUpgradeMediaTypesTest().mediaType(MediaType.APPLICATION_SERIALIZED_OBJECT).protocolVersion(PROTOCOL_VERSION_40),
      };
   }

   private HotRodUpgradeMediaTypesTest protocolVersion(ProtocolVersion version) {
      this.version = version;
      return this;
   }

   private HotRodUpgradeMediaTypesTest mediaType(MediaType mediaType) {
      this.mediaType = mediaType;
      return this;
   }

   @Override
   protected String parameters() {
      return String.format("[mediaType=%s,version=%s]", mediaType, version.toString().replace(".", "_"));
   }

   @BeforeMethod
   public void setup() throws Exception {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.clustering().cacheMode(CacheMode.DIST_SYNC);
      if (mediaType != null) {
         config.encoding().mediaType(mediaType.toString());
      }
      sourceCluster = new TestCluster.Builder().setName("sourceCluster").setNumMembers(2)
            .cache().name(CACHE_NAME).configuredWith(config)
            .build();

      targetCluster = new TestCluster.Builder().setName("targetCluster").setNumMembers(2)
            .cache().name(CACHE_NAME).configuredWith(config)
            .remoteProtocolVersion(version)
            .remotePort(sourceCluster.getHotRodPort())
            .remoteStoreProperty(RemoteStore.MIGRATION, "true")
            .build();

   }

   public void testSynchronization() throws Exception {
      RemoteCache<Object, Object> sourceRemoteCache = sourceCluster.getRemoteCache(CACHE_NAME, mediaType);
      RemoteCache<Object, Object> targetRemoteCache = targetCluster.getRemoteCache(CACHE_NAME, mediaType);

      for (int i = 0; i < ENTRIES; i++) {
         sourceRemoteCache.put(key(i), value(i));
      }

      assertEquals(ENTRIES, sourceRemoteCache.size());
      assertEquals(ENTRIES, targetRemoteCache.size());
      assertEquals(value(5), targetRemoteCache.get(key(5)));

      // Do a Rolling Upgrade
      RollingUpgradeManager upgradeManager = targetCluster.getRollingUpgradeManager(CACHE_NAME);
      long count = upgradeManager.synchronizeData("hotrod");
      assertEquals(ENTRIES, count);

      // Disconnect remote store
      upgradeManager.disconnectSource("hotrod");

      // Check migrated data
      assertEquals(sourceCluster.getEmbeddedCache(CACHE_NAME).size(), targetCluster.getEmbeddedCache(CACHE_NAME).size());
      assertEquals(value(7), targetRemoteCache.get(key(7)));
   }

   @AfterMethod
   public void tearDown() {
      sourceCluster.destroy();
      targetCluster.destroy();
   }

   private Object key(int idx) {
      return idx;
   }

   private Object value(int idx) {
      return "Value_" + idx;
   }
}
