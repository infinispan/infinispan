package org.infinispan.client.hotrod.retry;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.security.simple.SimpleServerAuthenticationProvider;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.TestCallbackHandler;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.retry.SecureServerFailureRetryTest")
public class SecureServerFailureRetryTest extends ServerFailureRetryTest {

   @Override
   protected HotRodServer createStartHotRodServer(EmbeddedCacheManager manager) {
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      SimpleServerAuthenticationProvider sap = new SimpleServerAuthenticationProvider();
      sap.addUser("user", "realm", "password".toCharArray(), null);
      serverBuilder.authentication()
         .enable()
         .serverName("localhost")
         .addAllowedMech("CRAM-MD5")
         .serverAuthenticationProvider(sap);
      return HotRodClientTestingUtil.startHotRodServer(manager, serverBuilder);
   }

   @Override
   protected RemoteCacheManager createRemoteCacheManager(int port) {
      ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder
         .security().authentication()
            .enable()
            .saslMechanism("CRAM-MD5")
            .callbackHandler(new TestCallbackHandler("user", "realm", "password".toCharArray()))
         .forceReturnValues(true)
         .connectionTimeout(5)
         .connectionPool().maxActive(1)
         .addServer().host("127.0.0.1").port(port);
      return new InternalRemoteCacheManager(clientBuilder.build());
   }

}
