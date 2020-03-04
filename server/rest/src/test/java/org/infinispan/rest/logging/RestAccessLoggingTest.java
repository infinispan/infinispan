package org.infinispan.rest.logging;

import static org.testng.AssertJUnit.assertTrue;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.infinispan.commons.test.skip.StringLogAppender;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.commons.test.TestResourceTracker;
import org.testng.annotations.Test;

/**
 * @since 9.2
 */
@Test(groups = "functional", testName = "rest.RestAccessLoggingTest")
public class RestAccessLoggingTest extends SingleCacheManagerTest {
   private static final String LOG_FORMAT = "%X{address} %X{user} [%d{dd/MMM/yyyy:HH:mm:ss Z}] \"%X{method} %m %X{protocol}\" %X{status} %X{requestSize} %X{responseSize} %X{duration} %X{h:User-Agent}";
   private StringLogAppender logAppender;
   private String testShortName;
   private RestServerHelper restServer;
   private HttpClient client;

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      return TestCacheManagerFactory.createCacheManager();
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      testShortName = TestResourceTracker.getCurrentTestShortName();
      logAppender = new StringLogAppender("org.infinispan.REST_ACCESS_LOG",
            Level.TRACE,
            t -> t.getName().startsWith("REST-" + testShortName + "-ServerIO-"),
            PatternLayout.newBuilder().withPattern(LOG_FORMAT).build());
      logAppender.install();
      restServer = new RestServerHelper(cacheManager);
      restServer.start(TestResourceTracker.getCurrentTestShortName());
      client = new HttpClient();
      client.start();
   }

   @Override
   protected void teardown() {
      try {
         logAppender.uninstall();
         client.stop();
         restServer.stop();
      } catch (Exception e) {

      }
      super.teardown();
   }

   public void testRestAccessLog() throws Exception {
      client.newRequest(String.format("http://localhost:%d/rest/v2/caches/default/key", restServer.getPort()))
            .content(new StringContentProvider("value"))
            .header("Content-type", "text/plain; charset=utf-8")
            .method(HttpMethod.PUT)
            .send();

      restServer.stop();

      String logline = logAppender.getLog(0);

      assertTrue(logline, logline.matches("^127\\.0\\.0\\.1 - \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d+] \"PUT /rest/v2/caches/default/key HTTP/1\\.1\" 404 \\d+ \\d+ \\d+ Jetty/\\p{Graph}+$"));
   }
}
