package org.infinispan.server.resp.logging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.test.skip.StringLogAppender;
import org.infinispan.server.resp.SingleNodeRespBaseTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import io.lettuce.core.api.sync.RedisCommands;

@Test(groups = "functional", testName = "server.resp.logging.RespAccessLoggingTest")
public class RespAccessLoggingTest extends SingleNodeRespBaseTest {

   public static final String LOG_FORMAT = "%X{address} %X{user} [%d{dd/MMM/yyyy:HH:mm:ss Z}] \"%X{method} %m %X{protocol}\" %X{status} %X{requestSize} %X{responseSize} %X{duration}";
   StringLogAppender logAppender;
   private String testShortName;

   @Override
   protected void afterSetupFinished() {
      testShortName = TestResourceTracker.getCurrentTestShortName();

      logAppender = new StringLogAppender(RespAccessLogger.log.getName(),
            Level.TRACE,
            t -> t.getName().startsWith("non-blocking-thread-" + testShortName),
            PatternLayout.newBuilder().withPattern(LOG_FORMAT).build());
      logAppender.install();
      assertThat(RespAccessLogger.isEnabled()).isTrue();
      super.afterSetupFinished();
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      logAppender.uninstall();
      super.destroy();
   }

   @Test
   public void testAccessLog() {
      int size = 10;
      // This will send the HELLO command.
      // Based on the response, Lettuce will send two CLIENT SETINFO requests.
      // So, creating the connection issues 3 requests in total.
      RedisCommands<String, String> redis = redisConnection.sync();

      for (int i = 0; i < size; i ++) {
         redis.set("key" + i, "value" + i);
      }

      // INFO has a big response.
      redis.info();

      // MGET uses multiple keys.
      redis.mget("key1", "key2", "key3");

      // MSET uses multiple keys with values.
      redis.mset(Map.of("k1", "v1", "k2", "v2", "k3", "v3"));

      // Unknown command.
      assertThatThrownBy(() -> redis.xadd("something", "kv1", "kv2"))
            .hasMessageContaining("unknown command");

      server.getTransport().stop();

      assertThat(logAppender.get(0))
            .matches("^127\\.0\\.0\\.1 - \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d*] \"HELLO /\\[] RESP\" OK \\d+ \\d+ \\d+$");

      assertThat(logAppender.get(1))
            .matches("^127\\.0\\.0\\.1 - \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d*] \"CLIENT /\\[] RESP\" OK \\d+ \\d+ \\d+$");
      assertThat(logAppender.get(2))
            .matches("^127\\.0\\.0\\.1 - \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d*] \"CLIENT /\\[] RESP\" OK \\d+ \\d+ \\d+$");

      size += 2;
      int i = 3;

      for (; i <= size; i ++) {
         String logLine = logAppender.get(i);
         assertThat(logLine)
               .matches("^127\\.0\\.0\\.1 - \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d*] \"SET /\\[\\[B0x\\w+] RESP\" OK \\d+ \\d+ \\d+$");
      }

      assertThat(logAppender.get(size + 1))
            // INFO writes more than 4K data to the buffer
            .matches("^127\\.0\\.0\\.1 - \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d*] \"INFO /\\[] RESP\" OK \\d+ 4\\d{3} \\d+$");

      assertThat(logAppender.get(size + 2))
            // We invoke MGET with 3 keys.
            .matches("^127\\.0\\.0\\.1 - \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d*] \"MGET /\\[?(\\[B0x\\w+[,\\]]){3} RESP\" OK \\d+ \\d+ \\d+$");

      assertThat(logAppender.get(size + 3))
            // We invoke MSET with 3 keys.
            .matches("^127\\.0\\.0\\.1 - \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d*] \"MSET /\\[?(\\[B0x\\w+[,\\]]){3} RESP\" OK \\d+ \\d+ \\d+$");

      assertThat(logAppender.get(size + 4))
            // We invoke an unknown command.
            .matches("^127\\.0\\.0\\.1 - \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d*] \"UNKNOWN_COMMAND /\\[] RESP\" OK \\d+ \\d+ \\d+$");
   }
}
