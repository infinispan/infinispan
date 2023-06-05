package org.infinispan.rest.logging;

import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.testng.AssertJUnit.assertTrue;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.test.skip.StringLogAppender;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
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
   private RestClient restClient;
   private RestCacheClient cacheClient;

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
            t -> t.getName().startsWith("non-blocking-thread-" + testShortName),
            PatternLayout.newBuilder().withPattern(LOG_FORMAT).build());
      logAppender.install();
      restServer = new RestServerHelper(cacheManager);
      restServer.start(TestResourceTracker.getCurrentTestShortName());
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.addServer().host(restServer.getHost()).port(restServer.getPort());
      restClient = RestClient.forConfiguration(builder.create());
      cacheClient = restClient.cache("default");
   }

   @Override
   protected void teardown() {
      try {
         logAppender.uninstall();
         restClient.close();
         restServer.stop();
      } catch (Exception ignored) {
      }
      super.teardown();
   }

   public void testRestAccessLog() {
      join(cacheClient.put("key", "value"));

      restServer.stop();

      String logline = logAppender.getLog(0);

      assertTrue(logline, logline.matches("^127\\.0\\.0\\.1 - \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d+] \"PUT /rest/v2/caches/default/key HTTP/1\\.1\" 404 \\d+ \\d+ \\d+ Infinispan/\\p{Graph}+$"));
   }
}
