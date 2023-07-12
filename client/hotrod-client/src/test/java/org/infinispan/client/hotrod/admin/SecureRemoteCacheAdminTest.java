package org.infinispan.client.hotrod.admin;

import org.infinispan.client.hotrod.DefaultTemplate;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.mappers.IdentityRoleMapper;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.core.security.simple.SimpleSaslAuthenticator;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.admin.SecureRemoteCacheAdminTest")
public class SecureRemoteCacheAdminTest extends RemoteCacheAdminTest {

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(String host, int serverPort) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder = super.createHotRodClientConfigurationBuilder(host, serverPort);
      builder.security().authentication().enable().saslMechanism("CRAM-MD5").username("admin").password("password");
      return builder;
   }

   @Override
   protected HotRodServer addHotRodServer(ConfigurationBuilder builder) {
      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb.defaultCacheName("default");
      gcb.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
      gcb.security().authorization().enable().groupOnlyMapping(false).principalRoleMapper(new IdentityRoleMapper()).role("admin").permission(AuthorizationPermission.ALL);
      gcb.serialization().addContextInitializer(contextInitializer());

      ConfigurationBuilder template = new ConfigurationBuilder();
      template.read(builder.build());
      template.security().authorization().role("admin");

      EmbeddedCacheManager cm = Security.doPrivileged(() -> {
         EmbeddedCacheManager cacheManager = addClusterEnabledCacheManager(gcb, builder);
         cacheManager.defineConfiguration("template", builder.build());
         cacheManager.defineConfiguration(DefaultTemplate.DIST_ASYNC.getTemplateName(), builder.build());
         return cacheManager;
      });

      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
      SimpleSaslAuthenticator ssa = new SimpleSaslAuthenticator();
      ssa.addUser("admin", "realm", "password".toCharArray(), "admin");
      serverBuilder.authentication()
            .enable()
            .sasl()
            .authenticator(ssa)
            .serverName("localhost")
            .addAllowedMech("CRAM-MD5");
      HotRodServer server = Security.doPrivileged(() -> HotRodClientTestingUtil.startHotRodServer(cm, serverBuilder));
      servers.add(server);
      return server;
   }
}
