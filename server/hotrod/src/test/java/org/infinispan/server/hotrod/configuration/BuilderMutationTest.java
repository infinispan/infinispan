package org.infinispan.server.hotrod.configuration;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "functional", testName = "server.hotrod.configuration.BuilderMutationTest")
public class BuilderMutationTest {

   public void testMutatePortAndHost() {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();

      builder.host("foo").port(1234);
      assertHostPort(builder.build(), "foo", 1234);

      builder.host("bar").port(4321);
      assertHostPort(builder.build(), "bar", 4321);
   }

   private void assertHostPort(HotRodServerConfiguration configuration, String host, int port) {
      assertEquals(configuration.host(), host);
      assertEquals(configuration.proxyHost(), null);
      assertEquals(configuration.publicHost(), host);
      assertEquals(configuration.port(), port);
      assertEquals(configuration.proxyPort(), -1);
      assertEquals(configuration.publicPort(), port);

   }
}
