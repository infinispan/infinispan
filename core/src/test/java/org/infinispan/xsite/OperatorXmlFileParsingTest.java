package org.infinispan.xsite;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.configuration.global.JGroupsConfiguration;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.jgroups.conf.ProtocolConfiguration;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "xsite.OperatorXmlFileParsingTest")
public class OperatorXmlFileParsingTest extends AbstractInfinispanTest {

   private static final String FILE_NAME = "configs/xsite/operator-similar.xml";

   // NOTE: if some protocol is updated in the default TCP or UDP stack, these lists need to be updated as well!!
   private static final List<String> IMAGE_TCP_STACK = Arrays.asList(
         "TCP",
         "RED",
         "dns.DNS_PING",
         "MERGE3",
         "FD_SOCK2",
         "FD_ALL3",
         "VERIFY_SUSPECT2",
         "pbcast.NAKACK2",
         "UNICAST3",
         "pbcast.STABLE",
         "pbcast.GMS",
         "UFC",
         "MFC",
         "FRAG4"
   );
   private static final List<String> XSITE_STACK = Arrays.asList(
         "TCP",
         "RED",
         "dns.DNS_PING",
         "MERGE3",
         "FD_SOCK2",
         "FD_ALL3",
         "VERIFY_SUSPECT2",
         "pbcast.NAKACK2",
         "UNICAST3",
         "pbcast.STABLE",
         "pbcast.GMS",
         "UFC",
         "MFC",
         "FRAG4",
         "relay.RELAY2"
   );
   private static final List<String> RELAY_TUNNEL_STACK = Arrays.asList(
         "TUNNEL",
         "RED",
         "PING",
         "MERGE3",
         "FD_ALL3",
         "VERIFY_SUSPECT2",
         "pbcast.NAKACK2",
         "UNICAST3",
         "pbcast.STABLE",
         "pbcast.GMS",
         "UFC",
         "MFC",
         "FRAG4"

   );

   public void testJGroupsStacks() throws IOException {
      JGroupsConfiguration jGroupsConfiguration = TestCacheManagerFactory.parseFile(FILE_NAME, false)
            .getGlobalConfigurationBuilder()
            .build()
            .transport()
            .jgroups();
      assertJGroupsStack(jGroupsConfiguration, "image-tcp", IMAGE_TCP_STACK);
      assertJGroupsStack(jGroupsConfiguration, "xsite", XSITE_STACK);
      assertJGroupsStack(jGroupsConfiguration, "relay-tunnel", RELAY_TUNNEL_STACK);
   }

   private void assertJGroupsStack(JGroupsConfiguration jGroupsConfiguration, String stack, List<String> expected) {
      List<String> parsed = jGroupsConfiguration.configurator(stack)
            .getProtocolStack()
            .stream()
            .map(ProtocolConfiguration::getProtocolName)
            .collect(Collectors.toList());
      AssertJUnit.assertEquals("Incorrect stack: " + stack, expected, parsed);
   }

}
