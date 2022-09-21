package org.infinispan.server.resp;

import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.server.resp.configuration.RespServerConfigurationBuilder;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test single node with authentication enabled.
 *
 * @author Jose Bolina
 * @since 14.0
 */
@CleanupAfterMethod
@Test(groups = "functional", testName = "server.resp.RespAuthSingleNodeTest")
public class RespAuthSingleNodeTest extends RespSingleNodeTest {
   private static final String USERNAME = "admin";
   private static final String PASSWORD = "super-password";

   @Override
   protected RespServerConfigurationBuilder serverConfiguration() {
      RespServerConfigurationBuilder builder = super.serverConfiguration();
      builder.authentication()
            .enable()
            .authenticator(new FakeRespAuthenticator());
      return builder;
   }

   @BeforeMethod(alwaysRun = true)
   public void authenticateBefore(Method method) {
      if (method.getName().endsWith("Auth")) return;

      RedisCommands<String, String> redis = redisConnection.sync();
      redis.auth(USERNAME, PASSWORD);
   }

   @Override
   protected void teardown() {
      super.destroyAfterClass();
   }

   @Override
   public void testAuth() {
      // Try auth with wrong user/pass
      super.testAuth();

      // This method did not issue AUTH, so this should be unauthorized.
      RedisCommands<String, String> redis = redisConnection.sync();
      Exceptions.expectException(RedisCommandExecutionException.class,
            "WRONGPASS invalid username-password pair or user is disabled\\.",
            () -> redis.set("k", "v"));
   }

   @Override
   protected RedisPubSubCommands<String, String> createPubSubConnection() {
      RedisPubSubCommands<String, String> connection = super.createPubSubConnection();
      connection.auth(USERNAME, PASSWORD);
      return connection;
   }

   public static class FakeRespAuthenticator implements Authenticator {
      @Override
      public CompletionStage<Subject> authenticate(String username, char[] password) {
         if (username.equals(USERNAME) && new String(password).equals(PASSWORD)) {
            return CompletableFuture.completedFuture(new Subject());
         }
         return CompletableFutures.completedNull();
      }
   }
}
