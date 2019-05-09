package org.infinispan.server.hotrod.logging;

import static org.testng.AssertJUnit.assertTrue;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.infinispan.commons.test.skip.StringLogAppender;
import org.infinispan.server.hotrod.HotRodSingleNodeTest;
import org.testng.annotations.Test;

/**
 * @since 9.2
 */
@Test(groups = "functional", testName = "server.hotrod.logging.HotRodAccessLoggingTest")
public class HotRodAccessLoggingTest extends HotRodSingleNodeTest {
   public static final String LOG_FORMAT = "%X{address} %X{user} [%d{dd/MMM/yyyy:HH:mm:ss z}] \"%X{method} %m %X{protocol}\" %X{status} %X{requestSize} %X{responseSize} %X{duration}";
   StringLogAppender logAppender;

   @Override
   protected void setup() throws Exception {
      logAppender = new StringLogAppender("org.infinispan.HOTROD_ACCESS_LOG",
            Level.TRACE,
            t -> t.getName().startsWith("Hot Rod-HotRodAccessLoggingTest-ServerIO"),
            PatternLayout.newBuilder().withPattern(LOG_FORMAT).build());
      logAppender.install();
      super.setup();
   }

   @Override
   protected void teardown() {
      logAppender.uninstall();
      super.teardown();
   }

   public void testHotRodAccessLog() {
      client().put("key", "value");

      server().getTransport().stop();

      String logline = logAppender.getLog(0);
      assertTrue(logline, logline.matches("^127\\.0\\.0\\.1 - \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\w+\\] \"PUT /HotRodCache/\\[B0x6B6579 HOTROD/2\\.1\" OK \\d+ \\d+ \\d+$"));
   }
}
