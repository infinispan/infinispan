package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.server.resp.test.RespTestingUtil.ADMIN;
import static org.infinispan.server.resp.test.RespTestingUtil.HOST;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.testing.Testing.tmpDirectory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.mappers.IdentityRoleMapper;
import org.infinispan.server.resp.configuration.RespServerConfigurationBuilder;
import org.infinispan.server.resp.test.RespTestingUtil;
import org.infinispan.server.resp.test.SimpleRespAuthenticator;
import org.infinispan.server.resp.test.TestSetup;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.protocol.ProtocolVersion;

@Test(groups = "functional", testName = "server.resp.ScriptingAuthorizationTest")
public class ScriptingAuthorizationTest extends MultipleCacheManagersTest {
   private RespServer server;

   @Override
   protected void createCacheManagers() {
      SimpleRespAuthenticator authenticator = new SimpleRespAuthenticator();
      authenticator.addUser("execuser", "execuser");
      authenticator.addUser("readuser", "readuser");

      cacheManagers.add(Security.doAs(ADMIN, () ->
            TestSetup.singleNodeTestSetup().createCacheManager(
                  this::defaultRespConfiguration,
                  this::configureGlobalAuthorization,
                  builder -> {})));

      RespServerConfigurationBuilder builder = new RespServerConfigurationBuilder();
      builder.host(HOST).port(RespTestingUtil.port()).defaultCacheName("default")
            .authentication().enable().authenticator(authenticator);

      server = Security.doAs(ADMIN, () -> RespTestingUtil.startServer(cacheManagers.get(0), builder.build()));
   }

   private void configureGlobalAuthorization(GlobalConfigurationBuilder builder) {
      // Enable global state (required for the RESP server)
      String stateDirectory = tmpDirectory(this.getClass().getSimpleName());
      Util.recursiveFileRemove(stateDirectory);
      builder.globalState().enable()
            .persistentLocation(stateDirectory)
            .configurationStorage(ConfigurationStorage.OVERLAY)
            .sharedPersistentLocation(stateDirectory);

      // Configure authorization
      var authz = builder.security().authorization().enable()
            .groupOnlyMapping(false)
            .principalRoleMapper(new IdentityRoleMapper());

      // Role for all permissions (used by ADMIN during setup)
      var allRole = authz.role("ALL");
      for (AuthorizationPermission perm : AuthorizationPermission.values()) {
         allRole.permission(perm);
      }

      // Role for execuser: can read, write, execute scripts, and has admin access
      authz.role("execuser")
            .permission(AuthorizationPermission.READ)
            .permission(AuthorizationPermission.WRITE)
            .permission(AuthorizationPermission.EXEC)
            .permission(AuthorizationPermission.CREATE)
            .permission(AuthorizationPermission.ADMIN);

      // Role for readuser: can only read and has admin access (but not EXEC)
      authz.role("readuser")
            .permission(AuthorizationPermission.READ)
            .permission(AuthorizationPermission.ADMIN);
   }

   protected ConfigurationBuilder defaultRespConfiguration() {
      ConfigurationBuilder builder = RespTestingUtil.defaultRespConfiguration();
      builder.encoding().mediaType(MediaType.APPLICATION_OCTET_STREAM);
      builder.security().authorization().enable();
      return builder;
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      if (server != null) server.stop();
      super.destroy();
   }

   private RedisClient createClient(String username, String password) {
      RedisURI.Builder uriBuilder = RedisURI.Builder
            .redis(server.getHost(), server.getPort())
            .withTimeout(Duration.ofMillis(15_000))
            .withAuthentication(username, password);

      RedisClient client = RedisClient.create(uriBuilder.build());
      client.setOptions(ClientOptions.builder()
            .protocolVersion(ProtocolVersion.RESP3)
            .timeoutOptions(TimeoutOptions.enabled(Duration.of(15_000, ChronoUnit.MILLIS)))
            .build());
      return client;
   }

   @Test
   public void testEvalAllowedWithExecPermission() {
      try (RedisClient client = createClient("execuser", "execuser");
           StatefulRedisConnection<String, String> conn = client.connect()) {
         RedisCommands<String, String> redis = conn.sync();
         String out = redis.eval("return 'hello'", ScriptOutputType.VALUE);
         assertThat(out).isEqualTo("hello");
      }
   }

   @Test
   public void testEvalShaAllowedWithExecPermission() {
      try (RedisClient client = createClient("execuser", "execuser");
           StatefulRedisConnection<String, String> conn = client.connect()) {
         RedisCommands<String, String> redis = conn.sync();
         String sha = redis.scriptLoad("return 'hello'");
         String out = redis.evalsha(sha, ScriptOutputType.VALUE, new String[0]);
         assertThat(out).isEqualTo("hello");
      }
   }

   @Test
   public void testEvalDeniedWithoutExecPermission() {
      try (RedisClient client = createClient("readuser", "readuser");
           StatefulRedisConnection<String, String> conn = client.connect()) {
         RedisCommands<String, String> redis = conn.sync();
         assertThatThrownBy(() ->
               redis.eval("""
                     return redis.call('get', KEYS[1])
                     """, ScriptOutputType.VALUE, new String[]{k()}))
               .isInstanceOf(RedisCommandExecutionException.class)
               .hasMessageContaining("Unauthorized access")
               .hasMessageContaining("lacks")
               .hasMessageContaining("permission");
      }
   }

   @Test
   public void testEvalRoDeniedWithoutExecPermission() {
      try (RedisClient client = createClient("readuser", "readuser");
           StatefulRedisConnection<String, String> conn = client.connect()) {
         RedisCommands<String, String> redis = conn.sync();
         assertThatThrownBy(() ->
               redis.evalReadOnly("""
                     return redis.call('get', KEYS[1])
                     """, ScriptOutputType.VALUE, new String[]{k()}))
               .isInstanceOf(RedisCommandExecutionException.class)
               .hasMessageContaining("Unauthorized access")
               .hasMessageContaining("lacks")
               .hasMessageContaining("permission");
      }
   }
}
