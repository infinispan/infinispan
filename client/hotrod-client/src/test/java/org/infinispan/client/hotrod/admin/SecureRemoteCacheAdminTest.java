package org.infinispan.client.hotrod.admin;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.impl.IdentityRoleMapper;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.core.security.simple.SimpleServerAuthenticationProvider;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.admin.SecureRemoteCacheAdminTest")
public class SecureRemoteCacheAdminTest extends RemoteCacheAdminTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      builder.indexing().index(Index.ALL).autoConfig(true);
      createHotRodServers(2, builder);
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(int serverPort) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder = super.createHotRodClientConfigurationBuilder(serverPort);
      builder.marshaller(new ProtoStreamMarshaller());
      builder.security().authentication().enable().saslMechanism("CRAM-MD5").username("admin").password("password");
      return builder;
   }

   @Override
   protected HotRodServer addHotRodServer(ConfigurationBuilder builder) {
      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb.defaultCacheName("default");
      gcb.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
      gcb.security().authorization().enable().principalRoleMapper(new IdentityRoleMapper()).role("admin").permission(AuthorizationPermission.ALL);

      ConfigurationBuilder template = new ConfigurationBuilder();
      template.read(builder.build());
      template.security().authorization().role("admin");

      try {
         EmbeddedCacheManager cm = Security.doPrivileged((PrivilegedExceptionAction<EmbeddedCacheManager>) () -> {
            EmbeddedCacheManager cacheManager = addClusterEnabledCacheManager(gcb, builder);
            cacheManager.defineConfiguration("template", builder.build());
            return cacheManager;
         });

         HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
         serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
         SimpleServerAuthenticationProvider sap = new SimpleServerAuthenticationProvider();
         sap.addUser("admin", "realm", "password".toCharArray(), "admin", ProtobufMetadataManager.SCHEMA_MANAGER_ROLE);
         serverBuilder.authentication()
               .enable()
               .serverAuthenticationProvider(sap)
               .serverName("localhost")
               .addAllowedMech("CRAM-MD5");
         HotRodServer server = Security.doPrivileged((PrivilegedExceptionAction<HotRodServer>) () -> HotRodClientTestingUtil.startHotRodServer(cm, serverBuilder));
         servers.add(server);
         return server;
      } catch (PrivilegedActionException e) {
         throw new RuntimeException(e);
      }
   }
}
