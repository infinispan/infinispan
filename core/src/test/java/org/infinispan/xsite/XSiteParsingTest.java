package org.infinispan.xsite;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.testng.AssertJUnit;
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
      String config = TestingUtil.wrapXMLWithSchema(
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
         AssertJUnit.assertTrue(TestingUtil.extractGlobalComponent(cm, Transport.class).isSiteCoordinator());
      }

   }

}
