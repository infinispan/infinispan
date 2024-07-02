package org.infinispan.test.fwk;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commons.jdkspecific.ThreadCreator;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.test.TestingUtil;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.relay.RELAY2;
import org.jgroups.protocols.relay.Relayer2;
import org.jgroups.protocols.relay.config.RelayConfig;

/**
 * RELAY2 only allows setting the bridge cluster name and properties via XML.
 *
 * This is a hack to change the bridge cluster name after the RELAY2 configuration is parsed, so that multiple
 * x-site tests can run in parallel.
 *
 * @author Dan Berindei
 * @since 9.2
 */
public class TEST_RELAY2 extends RELAY2 {
   static {
      ClassConfigurator.addProtocol((short) 1321, TEST_RELAY2.class);
   }

   @Override
   protected void parseSiteConfiguration(Map<String, RelayConfig.SiteConfig> map) throws Exception {
      super.parseSiteConfiguration(map);

      String testName = TestResourceTracker.getCurrentTestName();
      map.forEach((s, siteConfig) -> {
         List<RelayConfig.BridgeConfig> bridges = siteConfig.getBridges();
         for (int i = 0; i < bridges.size(); i++) {
            RelayConfig.BridgeConfig bridgeConfig = bridges.get(i);
            String config =
                  (String) TestingUtil.extractField(RelayConfig.PropertiesBridgeConfig.class, bridgeConfig, "config");
            // Keep the same ports for all the tests, just change the cluster name
            String clusterName = "bridge-" + (testName != null ? testName : "namenotset");
            bridges.set(i, new RelayConfig.PropertiesBridgeConfig(clusterName, config));
         }
      });
   }

   @Override
   protected void startRelayer(Relayer2 rel, String bridge_name) {
      // startRelayer blocks the carrier thread.
      // I suspect we are running ouf of threads in the common pool because we run tests in parallel.
      if (ThreadCreator.isVirtual(Thread.currentThread())) {
         CompletableFuture.runAsync(() -> super.startRelayer(rel, bridge_name));
      } else {
         super.startRelayer(rel, bridge_name);
      }
   }
}
