package org.infinispan.xsite;

import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.configuration.cache.BackupConfiguration.BackupStrategy;
import org.infinispan.configuration.cache.BackupConfigurationBuilder;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.jgroups.protocols.relay.RELAY2;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "xsite.XSiteInlineConfigFileParsingTest")
public class XSiteInlineConfigFileParsingTest extends SingleCacheManagerTest {

   public static final String FILE_NAME = "configs/xsite/xsite-inline-test.xml";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilderHolder holder = TestCacheManagerFactory.holderFromXml(FILE_NAME);
      TransportFlags flags = new TransportFlags().withPreserveConfig(true);
      return TestCacheManagerFactory.createClusteredCacheManager(false, holder, false, flags);
   }

   public void testInlineConfiguration() {
      GlobalConfiguration cmc = cacheManager.getCacheManagerConfiguration();
      assertEquals("LON", cmc.sites().localSite());
      JGroupsTransport transport = (JGroupsTransport) extractGlobalComponent(cacheManager, Transport.class);
      RELAY2 relay2 = transport.getChannel().getProtocolStack().findProtocol(RELAY2.class);
      assertEquals(3, relay2.getSites().size());
      assertTrue(relay2.getSites().contains("LON"));
      assertTrue(relay2.getSites().contains("SFO"));
      assertTrue(relay2.getSites().contains("NYC"));

      Configuration dcc = cacheManager.getDefaultCacheConfiguration();
      assertEquals(dcc.sites().allBackups().size(), 2);
      BackupConfigurationBuilder nyc = new BackupConfigurationBuilder(null).site("NYC").strategy(BackupStrategy.SYNC)
            .backupFailurePolicy(BackupFailurePolicy.IGNORE).failurePolicyClass(null).replicationTimeout(12003)
            .useTwoPhaseCommit(false).enabled(true);
      assertTrue(dcc.sites().allBackups().contains(nyc.create()));
      BackupConfigurationBuilder sfo = new BackupConfigurationBuilder(null).site("SFO").strategy(BackupStrategy.ASYNC)
            .backupFailurePolicy(BackupFailurePolicy.WARN).failurePolicyClass(null).replicationTimeout(15000)
            .useTwoPhaseCommit(false).enabled(true);
      assertTrue(dcc.sites().allBackups().contains(sfo.create()));
   }
}
