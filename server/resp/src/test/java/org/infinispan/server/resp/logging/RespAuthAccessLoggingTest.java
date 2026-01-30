package org.infinispan.server.resp.logging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.resp.test.RespTestingUtil.HOST;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.security.Security;
import org.infinispan.server.core.AbstractAuthAccessLoggingTest;
import org.infinispan.server.resp.RespServer;
import org.infinispan.server.resp.configuration.RespServerConfigurationBuilder;
import org.infinispan.server.resp.test.RespTestingUtil;
import org.infinispan.server.resp.test.SimpleRespAuthenticator;
import org.testng.annotations.Test;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.protocol.ProtocolVersion;

@Test(groups = "functional", testName = "server.resp.logging.AuthAccessLoggingTest")
public class RespAuthAccessLoggingTest extends AbstractAuthAccessLoggingTest {
   private RespServer server;

   @Override
   protected void setup() throws Exception {
      super.setup();
      assertThat(RespAccessLogger.isEnabled()).isTrue();
      SimpleRespAuthenticator authenticator = new SimpleRespAuthenticator();
      authenticator.addUser("writer", "writer");
      authenticator.addUser("reader", "reader");
      RespServerConfigurationBuilder builder = new RespServerConfigurationBuilder();
      builder.host(HOST).port(RespTestingUtil.port()).defaultCacheName("default")
            .authentication().enable().authenticator(authenticator);
      server = Security.doAs(ADMIN, () -> RespTestingUtil.startServer(cacheManager, builder.build()));
   }

   @Override
   protected void customCacheConfiguration(ConfigurationBuilder builder) {
      builder.encoding().mediaType(MediaType.APPLICATION_OCTET_STREAM);
   }

   @Override
   protected void teardown() {
      server.stop();
      super.teardown();
   }

   @Override
   protected String logCategory() {
      return RespAccessLogger.log.getName();
   }

   @Test
   public void testRespAccessLogging() {
      for (Map.Entry<String, String> user : USERS.entrySet()) {
         try (RedisClient client = createRespClient(user.getKey(), user.getValue())) {
            try (StatefulRedisConnection<String, String> connection = client.connect()) {
               RedisCommands<String, String> commands = connection.sync();
               try {
                  commands.set(k(0, user.getKey()), v());
               } catch (RedisCommandExecutionException e) {
                  if (!e.getMessage().startsWith("WRONGPASS")) {
                     throw e;
                  }
               }
               try {
                  commands.get(k(0, user.getKey()));
               } catch (RedisCommandExecutionException e) {
                  if (!e.getMessage().startsWith("WRONGPASS")) {
                     throw e;
                  }
               }
            } catch (RedisConnectionException e) {
               // ignore
            }
         }
      }

      assertEquals(12, logAppender.size());

      assertThat(parseAccessLog(0)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "RESP", "METHOD", "HELLO", "STATUS", "\"" + Messages.MESSAGES.noAuthHello() + "\"", "WHO", "-"));
      assertThat(parseAccessLog(1)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "RESP", "METHOD", "HELLO", "STATUS", "OK", "WHO", "writer"));
      assertThat(parseAccessLog(2)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "RESP", "METHOD", "CLIENT", "STATUS", "OK", "WHO", "writer"));
      assertThat(parseAccessLog(3)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "RESP", "METHOD", "CLIENT", "STATUS", "OK", "WHO", "writer"));
      assertThat(parseAccessLog(4)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "RESP", "METHOD", "SET", "STATUS", "OK", "WHO", "writer"));
      assertThat(parseAccessLog(5)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "RESP", "METHOD", "GET", "STATUS", "OK", "WHO", "writer"));
      assertThat(parseAccessLog(6)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "RESP", "METHOD", "HELLO", "STATUS", "OK", "WHO", "reader"));
      assertThat(parseAccessLog(7)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "RESP", "METHOD", "CLIENT", "STATUS", "OK", "WHO", "reader"));
      assertThat(parseAccessLog(8)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "RESP", "METHOD", "CLIENT", "STATUS", "OK", "WHO", "reader"));
      assertThat(parseAccessLog(9)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "RESP", "METHOD", "SET", "STATUS", "\"ISPN000287: Unauthorized access: subject 'Subject with principal(s): [SimpleUserPrincipal [name=reader]]' lacks 'WRITE' permission\"", "WHO", "reader"));
      assertThat(parseAccessLog(10)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "RESP", "METHOD", "GET", "STATUS", "OK", "WHO", "reader"));
      assertThat(parseAccessLog(11)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "RESP", "METHOD", "HELLO", "STATUS", "\"Unknown user\"", "WHO", "-"));
   }

   private RedisClient createRespClient(String username, String password) {
      RedisURI.Builder builder = RedisURI.Builder
            .redis(server.getHost(), server.getPort())
            .withTimeout(Duration.ofMillis(15_000));
      if (!username.isEmpty()) {
         builder.withAuthentication(username, password);
      }
      RedisClient client = RedisClient.create(builder.build());
      client.setOptions(ClientOptions.builder()
            .protocolVersion(ProtocolVersion.RESP3)
            .timeoutOptions(TimeoutOptions.enabled(Duration.of(15_000, ChronoUnit.MILLIS)))
            .build());
      return client;
   }
}
