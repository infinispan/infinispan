package org.infinispan.persistence.remote.upgrade;

import static org.infinispan.transaction.TransactionMode.TRANSACTIONAL;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.jboss.marshalling.commons.GenericJBossMarshaller;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Same as {@link HotRodUpgradePojoTest} but using remote store created dynamically.
 *
 * @since 13.0
 */
@Test(testName = "upgrade.hotrod.HotRodUpgradeDynamicPojoTest", groups = "functional")
public class HotRodUpgradeDynamicPojoTest extends HotRodUpgradePojoTest {

   @Factory
   public Object[] factory() {
      return new Object[]{
            new HotRodUpgradeDynamicPojoTest().autoCommit(false).transaction(TRANSACTIONAL),
            new HotRodUpgradeDynamicPojoTest().autoCommit(true).transaction(TRANSACTIONAL),
            new HotRodUpgradeDynamicPojoTest().transaction(TransactionMode.NON_TRANSACTIONAL),
      };
   }

   @Override
   protected TestCluster configureTargetCluster(ConfigurationBuilder cacheConfig) {
      return new TestCluster.Builder().setName("targetCluster").setNumMembers(2)
            .marshaller(GenericJBossMarshaller.class)
            .ctx(SerializationCtx.INSTANCE)
            .cache().name(CACHE_NAME).configuredWith(cacheConfig)
            .build();
   }

   @Override
   protected void connectTargetCluster() {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      RemoteStoreConfigurationBuilder storeBuilder = cfg.persistence().addStore(RemoteStoreConfigurationBuilder.class);
      storeBuilder.marshaller(GenericJBossMarshaller.class).remoteCacheName(CACHE_NAME).segmented(false).shared(true)
            .addServer().host("localhost").port(sourceCluster.getHotRodPort());
      targetCluster.connectSource(CACHE_NAME, cfg.build().persistence().stores().get(0));
   }
}
