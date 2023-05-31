package org.infinispan.configuration;

import static org.infinispan.commons.test.Exceptions.expectException;
import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.infinispan.test.TestingUtil.wrapXMLWithSchema;
import static org.testng.AssertJUnit.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

/**
 * Parsing tests for Cross-Site
 *
 * @since 14.0
 */
@Test(groups = "functional", testName = "xsite.XSiteParsingTest")
public class XSiteParsingTest extends AbstractInfinispanTest {

   // https://issues.redhat.com/browse/ISPN-13623 reproducer
   public void testMultipleStackParents() throws IOException {
      String config = wrapXMLWithSchema(
            "<jgroups>" +
                  "<stack name=\"parent\" extends=\"udp\">" +
                  "  <UDP mcast_port=\"54444\"/>" +
                  "</stack>" +
                  "<stack name=\"bridge\" extends=\"tcp\">" +
                  "  <MPING mcast_port=\"55555\" />" +
                  "</stack>" +
                  "<stack name=\"xsite\" extends=\"parent\">" +
                  "  <relay.RELAY2 site=\"a\" />" +
                  "  <remote-sites default-stack=\"bridge\">" +
                  "    <remote-site name=\"a\" />" +
                  "    <remote-site name=\"b\" />" +
                  "  </remote-sites>" +
                  "</stack>" +
                  "</jgroups>" +
                  "<cache-container>" +
                  "   <transport cluster=\"multiple-parent-stack\" stack=\"xsite\"/>" +
                  "</cache-container>"
      );

      try (DefaultCacheManager cm = new DefaultCacheManager(new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8)))) {
         // just to make sure the DefaultCacheManager starts.
         assertTrue(extractGlobalComponent(cm, Transport.class).isSiteCoordinator());
      }
   }

   public void testInvalidMaxTombstoneCleanupDelay() {
      String config1 = wrapXMLWithSchema(
            "<cache-container>" +
                  "   <transport/>" +
                  "      <distributed-cache name=\"A\">" +
                  "         <backups max-cleanup-delay=\"-1\"/>" +
                  "      </distributed-cache>" +
                  "</cache-container>"
      );
      assertCacheConfigurationException(config1, "A", "ISPN000951: Invalid value -1 for attribute max-cleanup-delay: must be a number greater than zero");
      String config2 = wrapXMLWithSchema(
            "<cache-container>" +
                  "   <transport/>" +
                  "      <distributed-cache name=\"B\">" +
                  "         <backups max-cleanup-delay=\"0\"/>" +
                  "      </distributed-cache>" +
                  "</cache-container>"
      );
      assertCacheConfigurationException(config2, "B", "ISPN000951: Invalid value 0 for attribute max-cleanup-delay: must be a number greater than zero");
   }

   private void assertCacheConfigurationException(String config, String cacheName, String messageRegex) {
      ParserRegistry parserRegistry = new ParserRegistry();
      ConfigurationBuilderHolder holder = parserRegistry.parse(config);
      GlobalConfiguration globalConfiguration = holder.getGlobalConfigurationBuilder().build();
      ConfigurationBuilder builder = holder.getNamedConfigurationBuilders().get(cacheName);
      expectException(CacheConfigurationException.class, messageRegex, builder::validate);
      expectException(CacheConfigurationException.class, messageRegex, () -> builder.validate(globalConfiguration));
   }

}
