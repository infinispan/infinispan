package org.infinispan.client.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.impl.CommonNameRoleMapper;
import org.infinispan.server.core.security.simple.SimpleServerAuthenticationProvider;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * @author Adrian Brock
 * @author Tristan Tarrant
 * @since 5.3
 */
@Test(testName = "client.hotrod.SslAuthenticationTest", groups = "functional")
public class SslAuthenticationTest extends SingleCacheManagerTest {

   private static final Log log = LogFactory.getLog(SslAuthenticationTest.class);
   static final Subject ADMIN = TestingUtil.makeSubject("CN=admin");
   public static final String UNAUTHORIZED = "unauthorized";

   private RemoteCacheManager remoteCacheManager;

   protected HotRodServer hotrodServer;


   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      global
         .security()
            .authorization()
               .enable()
               .principalRoleMapper(new CommonNameRoleMapper())
               .role("admin")
                  .permission(AuthorizationPermission.ALL)
               .role("HotRodClient1")
                  .permission(AuthorizationPermission.READ)
                  .permission(AuthorizationPermission.WRITE)
               .role("RodHot")
                  .permission(AuthorizationPermission.READ)
                  .permission(AuthorizationPermission.WRITE);
      org.infinispan.configuration.cache.ConfigurationBuilder builder = hotRodCacheConfiguration();
      builder.security().authorization().enable().role("HotRodClient1").role("admin");
      cacheManager = TestCacheManagerFactory.createCacheManager(global, builder);
      cacheManager.getCache();
      org.infinispan.configuration.cache.ConfigurationBuilder unauthorizedBuilder = hotRodCacheConfiguration();
      unauthorizedBuilder.security().authorization().enable().role("RodHot").role("admin");
      cacheManager.defineConfiguration(UNAUTHORIZED, unauthorizedBuilder.build());
      cacheManager.getCache(UNAUTHORIZED);

      return cacheManager;
   }

   @Override
   protected void setup() throws Exception {
      Security.doAs(ADMIN, (PrivilegedExceptionAction<Object>) () -> {
         cacheManager = createCacheManager();
         if (cache == null) cache = cacheManager.getCache();
         return null;
      });
      hotrodServer = new HotRodServer();
      HotRodServerConfigurationBuilder serverBuilder = HotRodTestingUtil.getDefaultHotRodConfiguration();

      ClassLoader cl = SslAuthenticationTest.class.getClassLoader();
      SimpleServerAuthenticationProvider sap = new SimpleServerAuthenticationProvider();
      serverBuilder
            .ssl()
               .enable()
               .requireClientAuth(true)
               .keyStoreFileName(cl.getResource("keystore_server.p12").getPath())
               .keyStorePassword("secret".toCharArray())
               .keyAlias("hotrod")
               .trustStoreFileName(cl.getResource("ca.p12").getPath())
               .trustStorePassword("secret".toCharArray());
      serverBuilder
            .authentication()
               .enable()
               .serverName("localhost")
               .addAllowedMech("EXTERNAL")
               .serverAuthenticationProvider(sap);
      Security.doAs(ADMIN, (PrivilegedExceptionAction<Object>) () -> {
         hotrodServer.start(serverBuilder.build(), cacheManager);
         return null;
      });

      log.info("Started server on port: " + hotrodServer.getPort());

      ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
      clientBuilder
            .addServer()
               .host("127.0.0.1")
               .port(hotrodServer.getPort())
            .socketTimeout(3000)
            .connectionPool()
               .maxActive(1)
               .timeBetweenEvictionRuns(2000)
            .security()
               .authentication()
                  .enable()
                  .saslMechanism("EXTERNAL")
               .ssl()
                  .enable()
                  .keyStoreFileName(cl.getResource("keystore_client.p12").getPath())
                  .keyStorePassword("secret".toCharArray())
                  .keyAlias("client1")
                  .trustStoreFileName(cl.getResource("ca.p12").getPath())
                  .trustStorePassword("secret".toCharArray());

      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
   }

   @Override
   protected void teardown() {
      HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
      HotRodClientTestingUtil.killServers(hotrodServer);
      super.teardown();
   }

   @Override
   protected void clearContent() {
      Security.doAs(ADMIN, (PrivilegedAction<Object>) () -> {
         cacheManager.getCache().clear();
         return null;
      });
   }


   public void testSSLAuthentication() throws Exception {
      RemoteCache<String, String> cache = remoteCacheManager.getCache();
      cache.put("k","v");
      assertEquals("v", cache.get("k"));
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*ISPN000287.*")
   public void testSSLUnauthorized() throws Exception {
      RemoteCache<String, String> cache = remoteCacheManager.getCache(UNAUTHORIZED);
      cache.put("k1","v1");
      assertEquals("v1", cache.get("k1"));
   }
}
