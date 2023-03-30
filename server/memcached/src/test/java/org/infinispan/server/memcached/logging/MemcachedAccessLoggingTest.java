package org.infinispan.server.memcached.logging;

import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.test.skip.StringLogAppender;
import org.infinispan.server.memcached.configuration.MemcachedProtocol;
import org.infinispan.server.memcached.test.MemcachedSingleNodeTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "server.memcached.logging.MemcachedAccessLoggingTest")
public class MemcachedAccessLoggingTest extends MemcachedSingleNodeTest {
   public static final String LOG_FORMAT = "%X{address} %X{user} [%d{dd/MMM/yyyy:HH:mm:ss Z}] \"%X{method} %m %X{protocol}\" %X{status} %X{requestSize} %X{responseSize} %X{duration}";
   StringLogAppender logAppender;
   private String testShortName;

   @Override
   protected void setup() throws Exception {
      testShortName = TestResourceTracker.getCurrentTestShortName();

      logAppender = new StringLogAppender(MemcachedAccessLogging.log.getName(),
            Level.TRACE,
            t -> t.getName().startsWith("non-blocking-thread-" + testShortName),
            PatternLayout.newBuilder().withPattern(LOG_FORMAT).build());
      logAppender.install();
      assertTrue(MemcachedAccessLogging.isEnabled());
      super.setup();
   }

   @Override
   protected void teardown() {
      logAppender.uninstall();
      super.teardown();
   }

   public void testMemcacheddAccessLog() throws ExecutionException, InterruptedException {
      client.set("key", 0, "value").get();

      server.getTransport().stop();

      String logline = logAppender.getLog(0);
      assertTrue(logline, logline.matches(
            "^127\\.0\\.0\\.1 - \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d*] \"set /key MCTXT\" OK \\d+ \\d+ \\d+$"));
   }

   @Override
   protected MemcachedProtocol getProtocol() {
      return MemcachedProtocol.TEXT;
   }
}
