package org.infinispan.server.memcached.logging;

import static org.infinispan.server.memcached.test.MemcachedTestingUtil.serverBuilder;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;

import javax.security.sasl.Sasl;

import org.infinispan.security.Security;
import org.infinispan.server.core.AbstractAuthAccessLoggingTest;
import org.infinispan.server.core.security.simple.SimpleAuthenticator;
import org.infinispan.server.memcached.MemcachedServer;
import org.infinispan.server.memcached.configuration.MemcachedProtocol;
import org.infinispan.server.memcached.configuration.MemcachedServerConfigurationBuilder;
import org.testng.annotations.Test;

import net.spy.memcached.ClientMode;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.auth.AuthDescriptor;

public abstract class MemcachedBaseAuthAccessLoggingTest extends AbstractAuthAccessLoggingTest {

   private MemcachedServer server;

   @Override
   protected String logCategory() {
      return MemcachedAccessLogging.log.getName();
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      assertTrue(MemcachedAccessLogging.isEnabled());

      SimpleAuthenticator sap = new SimpleAuthenticator();
      sap.addUser("writer", REALM, "writer".toCharArray());
      sap.addUser("reader", REALM, "reader".toCharArray());

      MemcachedServerConfigurationBuilder builder = serverBuilder().protocol(getProtocol()).defaultCacheName("default");
      builder.authentication().enable()
            .sasl().addAllowedMech("CRAM-MD5").authenticator(sap)
            .serverName("localhost").addMechProperty(Sasl.POLICY_NOANONYMOUS, "true");
      builder.authentication().text().authenticator(sap);
      server = new MemcachedServer();
      Security.doAs(ADMIN, () -> {
         server.start(builder.build(), cacheManager);
         server.postStart();
      });
   }

   @Override
   protected void teardown() {
      server.stop();
      super.teardown();
   }

   protected abstract MemcachedProtocol getProtocol();

   protected abstract void verifyLogs();

   @Test
   public void testAuthAccessLogging() throws Exception {
      for (Map.Entry<String, String> user : USERS.entrySet()) {
         MemcachedClient client = createClient(user.getKey(), user.getValue());
         try {
            client.set(k(0, user.getKey()), 0, v()).get();
         } catch (Exception e) {
            // Shutdown and recreate client
            client.shutdown();
            client = createClient(user.getKey(), user.getValue());
         }
         try {
            client.get(k(0, user.getKey()));
         } catch (Exception e) {}
         client.shutdown();
      }
      verifyLogs();
   }

   private MemcachedClient createClient(String username, String password) throws IOException {
      MemcachedProtocol protocol = getProtocol();
      ConnectionFactoryBuilder.Protocol p = protocol == MemcachedProtocol.BINARY ? ConnectionFactoryBuilder.Protocol.BINARY : ConnectionFactoryBuilder.Protocol.TEXT;
      ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder().setProtocol(p).setOpTimeout(10_000L);
      builder.setClientMode(ClientMode.Static).setFailureMode(FailureMode.Cancel);
      if (!username.isEmpty()) {
         builder.setAuthDescriptor(AuthDescriptor.typical(username, password));
      }
      return new MemcachedClient(builder.build(), Collections.singletonList(new InetSocketAddress(server.getHost(), server.getPort())));
   }
}
