package org.infinispan.server.resp.logging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.test.skip.StringLogAppender;
import org.infinispan.server.resp.SingleNodeRespBaseTest;
import org.infinispan.server.resp.test.RespTestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisConnectionException;

@Test(groups = "functional", testName = "server.resp.logging.AuthAccessLoggingTest")
public class RespAuthAccessLoggingTest extends SingleNodeRespBaseTest {

   {
      withAuthorization();
   }

   private StringLogAppender logAppender;
   private String testShortName;

   @Override
   protected void afterSetupFinished() {
      testShortName = TestResourceTracker.getCurrentTestShortName();

      logAppender = new StringLogAppender(RespAccessLogger.log.getName(),
            Level.TRACE,
            t -> t.getName().startsWith("non-blocking-thread-" + testShortName),
            PatternLayout.newBuilder().withPattern(RespAccessLoggingTest.LOG_FORMAT).build());
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
   public void testAuthInAccessLogging() {
      // Super already has a connection with an authenticated client.
      // Now, create a client without the auth configuration.
      try (RedisClient unauthorized = RespTestingUtil.createClient(timeout, server.getPort())) {
         assertThatThrownBy(() -> unauthorized.connect().sync())
               .isInstanceOf(RedisConnectionException.class)
               .cause()
               .hasMessageStartingWith("NOAUTH HELLO must be called with the client already authenticated");
      }

      server.getTransport().stop();

      assertThat(logAppender.getLog(0))
            .matches("^127\\.0\\.0\\.1 ALL_user \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d*] \"HELLO /\\[] RESP\" OK \\d+ \\d+ \\d+$");
      assertThat(logAppender.getLog(1))
            .matches("^127\\.0\\.0\\.1 ALL_user \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d*] \"CLIENT /\\[] RESP\" OK \\d+ \\d+ \\d+$");
      assertThat(logAppender.getLog(2))
            .matches("^127\\.0\\.0\\.1 ALL_user \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d*] \"CLIENT /\\[] RESP\" OK \\d+ \\d+ \\d+$");

      // The last entry is the failed authentication.
      // It doesn't have a user information.
      assertThat(logAppender.getLog(3))
            .matches("^127\\.0\\.0\\.1 - \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d*] \"HELLO /\\[] RESP\" NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time \\d+ \\d+ \\d+$");
   }
}
