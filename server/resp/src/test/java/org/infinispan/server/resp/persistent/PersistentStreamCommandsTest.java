package org.infinispan.server.resp.persistent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.server.resp.test.RespTestingUtil.HOST;
import static org.infinispan.server.resp.test.RespTestingUtil.startServer;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.resp.RespServer;
import org.infinispan.server.resp.test.RespTestingUtil;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.testing.Testing;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.ArrayOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;

/**
 * Verifies the stream data survives a server restart backed by a persistent cache store. These tests adapt the
 * upstream Redis tests that reload the stream from the RDB snapshot ({@code Empty stream can be rewrite into AOF
 * correctly} and {@code Stream can be rewrite into AOF correctly after XDEL lastid}), utilizing the Infinispan
 * persistence instead.
 *
 * @since 16.4
 */
@Test(groups = "functional", testName = "server.resp.persistent.PersistentStreamCommandsTest")
public class PersistentStreamCommandsTest extends AbstractInfinispanTest {

   private static final ProtocolKeyword XINFO_KEYWORD = keyword("XINFO");

   private EmbeddedCacheManager cacheManager;
   private RespServer server;
   private RedisClient client;
   private StatefulRedisConnection<String, String> connection;
   private int port;

   @BeforeMethod(alwaysRun = true)
   protected void beforeMethod() {
      Util.recursiveFileRemove(stateLocation());
      port = RespTestingUtil.port();
   }

   @AfterMethod(alwaysRun = true)
   protected void afterMethod() {
      stopServer();
   }

   @AfterClass(alwaysRun = true)
   protected void removeData() {
      Util.recursiveFileRemove(stateLocation());
   }

   /**
    * Upstream: Stream can be rewrite into AOF correctly after XDEL lastid. The stream entries and the last generated
    * ID survive the restart, so the XADD monotonicity continues and an ID smaller than the persisted last ID is
    * rejected.
    */
   public void testStreamSurvivesRestartAfterXdelLastEntry() {
      startRespServer();
      RedisCommands<String, String> redis = connection.sync();
      // The Lettuce client rejects a zero maxlen, so the raw command is utilized to create the stream.
      redis.dispatch(keyword("XADD"), new io.lettuce.core.output.ValueOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey("mystream").add("MAXLEN").add("0").add("*").add("a").add("b"));
      // Reset the last generated ID, as the upstream test does, so explicit IDs can be utilized.
      redis.dispatch(keyword("XSETID"), new io.lettuce.core.output.ValueOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey("mystream").add("0-0"));
      redis.xadd("mystream", new XAddArgs().id("1-1"), "a", "b");
      redis.xadd("mystream", new XAddArgs().id("2-2"), "a", "b");
      assertThat(redis.xlen("mystream")).isEqualTo(2);
      redis.xdel("mystream", "2-2");
      stopServer();

      startRespServer();
      RedisCommands<String, String> restarted = connection.sync();
      assertThat(restarted.xlen("mystream")).isEqualTo(1);
      assertThat(restarted.xrange("mystream", Range.unbounded()))
            .extracting(io.lettuce.core.StreamMessage::getId).containsExactly("1-1");
      assertThat(xinfo(restarted, "mystream").get("last-generated-id")).isEqualTo("2-2");

      // The ID monotonicity continues from the persisted last generated ID.
      assertThatThrownBy(() -> restarted.xadd("mystream", new XAddArgs().id("2-2"), "a", "b"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR");
      assertThat(restarted.xadd("mystream", new XAddArgs().id("3-3"), "a", "b")).isEqualTo("3-3");
   }

   /**
    * Upstream: Empty stream can be rewrite into AOF correctly. An empty stream remains empty, and the stream key
    * still exists, after the restart.
    */
   public void testEmptyStreamSurvivesRestart() {
      startRespServer();
      RedisCommands<String, String> redis = connection.sync();
      redis.dispatch(keyword("XADD"), new io.lettuce.core.output.ValueOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey("mystream").add("MAXLEN").add("0").add("*").add("a").add("b"));
      // Reset the last generated ID, as the upstream test does, so an explicit ID can be utilized after the restart.
      redis.dispatch(keyword("XSETID"), new io.lettuce.core.output.ValueOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey("mystream").add("0-0"));
      assertThat(xinfo(redis, "mystream")).containsEntry("length", 0L);
      assertThat(redis.xlen("mystream")).isEqualTo(0);
      stopServer();

      startRespServer();
      RedisCommands<String, String> restarted = connection.sync();
      assertThat(xinfo(restarted, "mystream")).containsEntry("length", 0L);
      assertThat(restarted.xlen("mystream")).isEqualTo(0);
      // The stream accepts new entries after the restart.
      assertThat(restarted.xadd("mystream", new XAddArgs().id("1-1"), "a", "b")).isEqualTo("1-1");
   }

   // --------------------------------------------------------
   // Server lifecycle on a persistent store
   // --------------------------------------------------------

   private void startRespServer() {
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
      TestCacheManagerFactory.amendGlobalConfiguration(globalBuilder, new TransportFlags());
      // Explicitly register the RESP serialization context with the stream types.
      globalBuilder.serialization().addContextInitializer(new org.infinispan.server.resp.GlobalContextInitializerImpl());
      // The global state and the persistent store share the same directory, which survives the restart within a
      // test method. The persisted configuration state is removed between restarts: the round-trip through the
      // persisted state would lose the RESP key partitioner, utilized to map the keys to the store segments.
      String stateDirectory = stateLocation();
      globalBuilder.globalState().enable()
            .persistentLocation(stateDirectory)
            .configurationStorage(org.infinispan.globalstate.ConfigurationStorage.OVERLAY)
            .sharedPersistentLocation(stateDirectory);

      ConfigurationBuilder builder = RespTestingUtil.defaultRespConfiguration();
      builder.persistence().addSoftIndexFileStore()
            .segmented(false)
            .dataLocation(dataLocation())
            .indexLocation(indexLocation());

      cacheManager = TestCacheManagerFactory.newDefaultCacheManager(true, globalBuilder, builder);
      // Each server start utilizes the next port in the thread range, as the previous one may still be in TIME_WAIT.
      server = startServer(cacheManager, port++);
      client = RedisClient.create(RedisURI.Builder.redis(HOST).withPort(server.getPort()).build());
      client.setOptions(io.lettuce.core.ClientOptions.builder()
            .protocolVersion(io.lettuce.core.protocol.ProtocolVersion.RESP3)
            .build());
      connection = client.connect();
   }

   private void stopServer() {
      if (connection != null) {
         connection.close();
         connection = null;
      }
      if (client != null) {
         client.shutdown();
         client = null;
      }
      if (server != null) {
         server.stop();
         server = null;
      }
      if (cacheManager != null) {
         TestingUtil.killCacheManagers(cacheManager);
         cacheManager = null;
         // Remove the persisted configuration state, keeping the persistent store data, so the next start recreates
         // the cache configuration instead of reading it back without the RESP key partitioner.
         File[] stateFiles = new File(stateLocation())
               .listFiles((dir, name) -> name.endsWith(".state"));
         if (stateFiles != null) {
            for (File stateFile : stateFiles) {
               stateFile.delete();
            }
         }
      }
   }

   private static Map<String, Object> xinfo(RedisCommands<String, String> redis, String key) {
      List<Object> raw = redis.dispatch(XINFO_KEYWORD, new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).add("STREAM").addKey(key));
      Map<String, Object> result = new java.util.HashMap<>();
      for (int i = 0; i < raw.size(); i += 2) {
         result.put(String.valueOf(raw.get(i)), raw.get(i + 1));
      }
      return result;
   }

   private static String stateLocation() {
      return Testing.tmpDirectory(PersistentStreamCommandsTest.class);
   }

   private static String dataLocation() {
      return Paths.get(stateLocation(), "data").toString();
   }

   private static String indexLocation() {
      return Paths.get(stateLocation(), "index").toString();
   }

   private static ProtocolKeyword keyword(String name) {
      return new ProtocolKeyword() {
         @Override
         public byte[] getBytes() {
            return name.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
         }

         @Override
         public String toString() {
            return name;
         }
      };
   }
}
