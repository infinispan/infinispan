package org.infinispan.server.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.RestServer;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.server.core.ProtocolServer;
import org.infinispan.server.core.ServerManagement;
import org.infinispan.server.core.ServerStateManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.testing.junit.JUnitThreadTrackerRule;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.wildfly.security.auth.permission.LoginPermission;
import org.wildfly.security.auth.realm.SimpleMapBackedSecurityRealm;
import org.wildfly.security.auth.realm.SimpleRealmEntry;
import org.wildfly.security.auth.server.MechanismConfiguration;
import org.wildfly.security.auth.server.MechanismConfigurationSelector;
import org.wildfly.security.auth.server.MechanismRealmConfiguration;
import org.wildfly.security.auth.server.SecurityDomain;
import org.wildfly.security.auth.server.http.HttpAuthenticationFactory;
import org.wildfly.security.credential.PasswordCredential;
import org.wildfly.security.http.HttpServerAuthenticationMechanismFactory;
import org.wildfly.security.http.util.FilterServerMechanismFactory;
import org.wildfly.security.http.util.SecurityProviderServerMechanismFactory;
import org.wildfly.security.http.util.SetMechanismInformationMechanismFactory;
import org.wildfly.security.password.PasswordFactory;
import org.wildfly.security.password.WildFlyElytronPasswordProvider;
import org.wildfly.security.password.interfaces.ClearPassword;
import org.wildfly.security.password.spec.ClearPasswordSpec;
import org.wildfly.security.permission.PermissionVerifier;

/**
 * Tests HTTP authentication mechanisms (Digest and LOCALUSER) including
 * client-side preauthentication support.
 *
 * @since 16.2
 */
public class HttpAuthenticationTest {

   @ClassRule
   public static final JUnitThreadTrackerRule tracker = new JUnitThreadTrackerRule();

   @org.junit.BeforeClass
   public static void disableNonceShutdown() {
      // Prevent DigestMechanismFactory.shutdown() from being called between tests,
      // which would corrupt shared static state and cause 500 errors in Digest tests.
      System.setProperty("infinispan.security.elytron.nonceshutdown", "false");
   }

   private RestServer restServer;
   private EmbeddedCacheManager cacheManager;
   private RestClient client;

   @After
   public void tearDown() throws Exception {
      Util.close(client);
      Util.close(restServer);
      Util.close(cacheManager);
   }

   private void startServer(ElytronHTTPAuthenticator authenticator, String... mechanisms) {
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().cacheManagerName("default");
      cacheManager = TestCacheManagerFactory.createCacheManager(globalBuilder, new ConfigurationBuilder());
      restServer = new RestServer();
      RestServerConfigurationBuilder builder = new RestServerConfigurationBuilder();
      builder.host("localhost").port(0).name("test");
      builder.authentication().authenticator(authenticator).addMechanisms(mechanisms);

      Map<String, ProtocolServer> protocolServers = new HashMap<>();
      ServerManagement serverManagement = createServerManagement(cacheManager, protocolServers);
      restServer.setServerManagement(serverManagement, true);
      restServer.start(builder.build(), cacheManager);
      restServer.postStart();
   }

   @SuppressWarnings("unchecked")
   private static ServerManagement createServerManagement(EmbeddedCacheManager cm, Map<String, ProtocolServer> protocolServers) {
      ServerManagement mgmt = mock(ServerManagement.class);
      when(mgmt.getCacheManager()).thenReturn((DefaultCacheManager) cm);
      when(mgmt.getProtocolServers()).thenReturn((Map) protocolServers);
      when(mgmt.getStatus()).thenReturn(org.infinispan.lifecycle.ComponentStatus.RUNNING);
      when(mgmt.getLoginConfiguration(org.mockito.ArgumentMatchers.any())).thenReturn(Collections.emptyMap());

      ServerStateManager ssm = mock(ServerStateManager.class);
      when(ssm.getIgnoredCaches()).thenReturn(Collections.emptySet());
      when(mgmt.getServerStateManager()).thenReturn(ssm);
      when(mgmt.getServerReport()).thenReturn(CompletableFuture.completedFuture(Path.of("")));
      when(mgmt.getUsers()).thenReturn(Collections.emptyMap());

      return mgmt;
   }

   private RestClient createClient(String mechanism) {
      return createClient(mechanism, null, null);
   }

   private RestClient createClient(String mechanism, String username, String password) {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.addServer().host(restServer.getHost()).port(restServer.getPort());
      builder.security().authentication().enable().mechanism(mechanism);
      if (username != null) {
         builder.security().authentication().username(username).password(password);
      }
      return RestClient.forConfiguration(builder.build());
   }

   private ElytronHTTPAuthenticator createDigestAuthenticator() throws Exception {
      SimpleMapBackedSecurityRealm realm = new SimpleMapBackedSecurityRealm();
      PasswordFactory passwordFactory = PasswordFactory.getInstance(ClearPassword.ALGORITHM_CLEAR, WildFlyElytronPasswordProvider.getInstance());
      Map<String, SimpleRealmEntry> users = new HashMap<>();
      users.put("admin", new SimpleRealmEntry(List.of(new PasswordCredential(
            passwordFactory.generatePassword(new ClearPasswordSpec("adminpass".toCharArray()))))));
      realm.setPasswordMap(users);

      SecurityDomain securityDomain = SecurityDomain.builder()
            .addRealm("default", realm).build()
            .setDefaultRealmName("default")
            .setPermissionMapper((principal, roles) -> PermissionVerifier.from(new LoginPermission()))
            .build();

      return createAuthenticator(securityDomain, "DIGEST");
   }

   private ElytronHTTPAuthenticator createLocalUserAuthenticator() throws Exception {
      SimpleMapBackedSecurityRealm realm = new SimpleMapBackedSecurityRealm();
      Map<String, SimpleRealmEntry> users = new HashMap<>();
      users.put("$local", new SimpleRealmEntry(List.of()));
      realm.setPasswordMap(users);

      SecurityDomain securityDomain = SecurityDomain.builder()
            .addRealm("default", realm).build()
            .setDefaultRealmName("default")
            .setPermissionMapper((principal, roles) -> PermissionVerifier.from(new LoginPermission()))
            .build();

      return createAuthenticator(securityDomain, "LOCALUSER");
   }

   private ElytronHTTPAuthenticator createMultiMechanismAuthenticator() throws Exception {
      SimpleMapBackedSecurityRealm realm = new SimpleMapBackedSecurityRealm();
      PasswordFactory passwordFactory = PasswordFactory.getInstance(ClearPassword.ALGORITHM_CLEAR, WildFlyElytronPasswordProvider.getInstance());
      Map<String, SimpleRealmEntry> users = new HashMap<>();
      users.put("admin", new SimpleRealmEntry(List.of(new PasswordCredential(
            passwordFactory.generatePassword(new ClearPasswordSpec("adminpass".toCharArray()))))));
      users.put("$local", new SimpleRealmEntry(List.of()));
      realm.setPasswordMap(users);

      SecurityDomain securityDomain = SecurityDomain.builder()
            .addRealm("default", realm).build()
            .setDefaultRealmName("default")
            .setPermissionMapper((principal, roles) -> PermissionVerifier.from(new LoginPermission()))
            .build();

      return createAuthenticator(securityDomain, "DIGEST", "LOCALUSER");
   }

   private ElytronHTTPAuthenticator createAuthenticator(SecurityDomain securityDomain, String... mechanisms) {
      ElytronHTTPAuthenticator authenticator = new ElytronHTTPAuthenticator("default", null, List.of(mechanisms));

      HttpServerAuthenticationMechanismFactory httpServerFactory = new SecurityProviderServerMechanismFactory(authenticator.getProviders());
      httpServerFactory = new SetMechanismInformationMechanismFactory(
            new FilterServerMechanismFactory(httpServerFactory, true, List.of(mechanisms)));

      MechanismConfiguration.Builder mechConfigBuilder = MechanismConfiguration.builder();
      mechConfigBuilder.addMechanismRealm(MechanismRealmConfiguration.builder().setRealmName("default").build());

      HttpAuthenticationFactory factory = HttpAuthenticationFactory.builder()
            .setSecurityDomain(securityDomain)
            .setFactory(httpServerFactory)
            .setMechanismConfigurationSelector(MechanismConfigurationSelector.constantSelector(mechConfigBuilder.build()))
            .build();

      authenticator.setFactory(factory);
      return authenticator;
   }

   @Test
   public void testDigestAuthenticationWithValidCredentials() throws Exception {
      startServer(createDigestAuthenticator(), "DIGEST");
      client = createClient("DIGEST", "admin", "adminpass");

      // Use non-anonymous endpoint to actually exercise auth
      RestResponse r = client.container().info().toCompletableFuture().get();
      assertThat(r.status()).isBetween(200, 204);
   }

   @Test
   public void testDigestPreauthentication() throws Exception {
      startServer(createDigestAuthenticator(), "DIGEST");
      client = createClient("DIGEST", "admin", "adminpass");

      // First request triggers full Digest challenge-response and caches the nonce
      RestResponse r1 = client.container().info().toCompletableFuture().get();
      assertThat(r1.status()).isBetween(200, 204);

      // Subsequent requests use preauthentication (cached nonce, no 401 round-trip)
      for (int i = 0; i < 5; i++) {
         RestResponse r = client.container().info().toCompletableFuture().get();
         assertThat(r.status()).isBetween(200, 204);
      }
   }

   @Test
   public void testDigestNoAuthentication() throws Exception {
      startServer(createDigestAuthenticator(), "DIGEST");

      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.addServer().host(restServer.getHost()).port(restServer.getPort());
      client = RestClient.forConfiguration(builder.build());

      // Health endpoint is accessible anonymously even when auth is configured
      RestResponse r = client.container().healthStatus().toCompletableFuture().get();
      assertThat(r.status()).isBetween(200, 204);
   }

   @Test
   public void testLocalUserAuthentication() throws Exception {
      startServer(createLocalUserAuthenticator(), "LOCALUSER");
      client = createClient("LOCALUSER");

      // Use non-anonymous endpoint to actually exercise LOCALUSER auth
      RestResponse r = client.container().info().toCompletableFuture().get();
      assertThat(r.status()).isBetween(200, 204);
   }

   @Test
   public void testLocalUserSessionTokenPreauthentication() throws Exception {
      startServer(createLocalUserAuthenticator(), "LOCALUSER");
      client = createClient("LOCALUSER");

      // First request: full 3-round-trip challenge-response
      RestResponse r1 = client.container().info().toCompletableFuture().get();
      assertThat(r1.status()).isBetween(200, 204);

      // Subsequent requests should also succeed
      for (int i = 0; i < 5; i++) {
         RestResponse r = client.container().info().toCompletableFuture().get();
         assertThat(r.status()).isBetween(200, 204);
      }
   }

   @Test
   public void testAutoDetectLocalUser() throws Exception {
      // Simulates the CLI flow: AUTO mechanism, no credentials, server with both DIGEST and LOCALUSER
      startServer(createMultiMechanismAuthenticator(), "DIGEST", "LOCALUSER");

      // Create client with AUTO mechanism and no credentials (like the CLI does)
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.addServer().host(restServer.getHost()).port(restServer.getPort());
      builder.security().authentication().enable();
      client = RestClient.forConfiguration(builder.build());

      // Access a non-anonymous endpoint - should auto-detect and use LOCALUSER
      RestResponse r = client.container().info().toCompletableFuture().get();
      assertThat(r.status()).isBetween(200, 204);
   }

   @Test
   public void testAutoDetectDigestWithCredentials() throws Exception {
      // Simulates the CLI flow: connect -u admin -p adminpass with AUTO mechanism
      // The client must NOT eagerly preauthenticate with Basic (which the server doesn't support)
      startServer(createMultiMechanismAuthenticator(), "DIGEST", "LOCALUSER");

      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.addServer().host(restServer.getHost()).port(restServer.getPort());
      builder.security().authentication().enable().username("admin").password("adminpass");
      client = RestClient.forConfiguration(builder.build());

      RestResponse r = client.container().info().toCompletableFuture().get();
      assertThat(r.status()).isBetween(200, 204);

      // Subsequent requests should use Digest preauthentication
      for (int i = 0; i < 3; i++) {
         RestResponse r2 = client.container().info().toCompletableFuture().get();
         assertThat(r2.status()).isBetween(200, 204);
      }
   }
}
