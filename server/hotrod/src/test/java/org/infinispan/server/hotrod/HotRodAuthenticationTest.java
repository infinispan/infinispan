package org.infinispan.server.hotrod;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServer;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.HashMap;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import org.infinispan.commons.util.SaslUtils;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.security.simple.SimpleAuthenticator;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.server.hotrod.test.TestAuthMechListResponse;
import org.infinispan.server.hotrod.test.TestAuthResponse;
import org.infinispan.server.hotrod.test.TestCallbackHandler;
import org.infinispan.server.hotrod.test.TestResponse;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import io.netty.channel.group.ChannelGroup;

/**
 * Hot Rod server authentication test.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@Test(groups = "functional", testName = "server.hotrod.HotRodAuthenticationTest")
public class HotRodAuthenticationTest extends HotRodSingleNodeTest {

   @Override
   public HotRodServer createStartHotRodServer(EmbeddedCacheManager cacheManager) {
      SimpleAuthenticator ssap = new SimpleAuthenticator();
      ssap.addUser("user", "realm", "password".toCharArray());
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      builder.authentication().enable()
            .sasl()
            .authenticator(ssap)
            .addAllowedMech("SCRAM-SHA-256")
            .serverName("localhost")
            .addMechProperty(Sasl.POLICY_NOANONYMOUS, "true");
      return startHotRodServer(cacheManager, HotRodTestingUtil.serverPort(), builder);
   }

   public void testAuthMechList() {
      TestAuthMechListResponse a = client().authMechList();
      assertEquals(1, a.mechs.size());
      assertTrue(a.mechs.contains("SCRAM-SHA-256"));
      assertEquals(1, server().getTransport().getNumberOfLocalConnections());
   }

   public void testAuth() throws SaslException {
      HashMap<String, String> props = new HashMap<>();
      SaslClient sc = SaslUtils.getSaslClientFactory(this.getClass().getClassLoader(), "SCRAM-SHA-256").createSaslClient(new String[]{"SCRAM-SHA-256"}, null, "hotrod", "localhost", props,
            new TestCallbackHandler("user", "realm", "password"));
      TestResponse res = client().auth(sc);
      assertThat(res).isInstanceOf(TestAuthResponse.class);
      assertEquals(1, server().getTransport().getNumberOfLocalConnections());
   }

   public void testUnauthorizedOpCloseConnection(Method m) {
      // Ensure the transport is clean
      ChannelGroup acceptedChannels =
            TestingUtil.extractField(server().getTransport(), "acceptedChannels");
      acceptedChannels.close().awaitUninterruptibly();
      try {
         client().assertPutFail(m);
      } finally {
         assertEquals(0, server().getTransport().getNumberOfLocalConnections());
      }
   }
}
