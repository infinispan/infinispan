package org.infinispan.client.hotrod.retry;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.security.simple.SimpleAuthenticator;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.TestCallbackHandler;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.retry.SecureServerFailureRetryTest")
public class SecureServerFailureRetryTest extends ServerFailureRetryTest {

   @Override
   protected HotRodServer createStartHotRodServer(EmbeddedCacheManager manager) {
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      SimpleAuthenticator sap = new SimpleAuthenticator();
      sap.addUser("user", "realm", "password".toCharArray(), null);
      serverBuilder.authentication()
         .enable()
            .sasl()
               .serverName("localhost")
               .addAllowedMech("SCRAM-SHA-256")
               .authenticator(sap);
      return HotRodClientTestingUtil.startHotRodServer(manager, serverBuilder);
   }

   @Override
   protected RemoteCacheManager createRemoteCacheManager(int port) {
      ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder
         .security().authentication()
            .enable()
            .saslMechanism("SCRAM-SHA-256")
            .callbackHandler(new TestCallbackHandler("user", "realm", "password".toCharArray()))
         .forceReturnValues(true)
         .connectionTimeout(5)
         .connectionPool().maxActive(1)
         .addServer().host("127.0.0.1").port(port);
      return new RemoteCacheManager(clientBuilder.build());
   }

}
