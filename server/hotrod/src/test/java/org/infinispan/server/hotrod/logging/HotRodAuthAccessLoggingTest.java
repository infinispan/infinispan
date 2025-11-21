package org.infinispan.server.hotrod.logging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServer;

import java.util.HashMap;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.test.skip.StringLogAppender;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.security.simple.SimpleSaslAuthenticator;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.HotRodSingleNodeTest;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.server.hotrod.test.TestCallbackHandler;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "server.hotrod.logging.HotRodAuthAccessLoggingTest")
public class HotRodAuthAccessLoggingTest extends HotRodSingleNodeTest {

   private StringLogAppender logAppender;
   private String testShortName;

   @Override
   protected void setup() throws Exception {
      testShortName = TestResourceTracker.getCurrentTestShortName();
      logAppender = new StringLogAppender("org.infinispan.HOTROD_ACCESS_LOG",
            Level.TRACE,
            t -> t.getName().startsWith("non-blocking-thread-" + testShortName),
            PatternLayout.newBuilder().withPattern(HotRodAccessLoggingTest.LOG_FORMAT).build());
      logAppender.install();
      super.setup();
   }

   @Override
   protected void teardown() {
      logAppender.uninstall();
      super.teardown();
   }

   @Override
   protected HotRodServer createStartHotRodServer(EmbeddedCacheManager cacheManager) {
      SimpleSaslAuthenticator ssap = new SimpleSaslAuthenticator();
      ssap.addUser("user", "realm", "password".toCharArray());
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      builder.authentication().enable()
            .sasl()
            .authenticator(ssap)
            .addAllowedMech("CRAM-MD5")
            .serverName("localhost")
            .addMechProperty(Sasl.POLICY_NOANONYMOUS, "true");
      return startHotRodServer(cacheManager, HotRodTestingUtil.serverPort(), builder);
   }

   public void testHotRodAccessLog() throws SaslException {
      client().put("k", "v");

      HashMap<String, String> props = new HashMap<>();
      SaslClient sc = Sasl.createSaslClient(new String[]{"CRAM-MD5"}, null, "hotrod", "localhost", props,
            new TestCallbackHandler("user", "realm", "password"));

      client().auth(sc);
      client().put("k", "v");

      server().getTransport().stop();

      // First, operation without authenticating.
      assertThat(logAppender.getLog(0))
            .matches("^127\\.0\\.0\\.1 - \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d*] \"PUT /" + getDefaultCacheName() + "/\\[B0x6B HOTROD/2\\.1\" java.lang.SecurityException: ISPN006017: Operation 'PUT' requires authentication \\d+ \\d+ \\d+$");

      // The client authenticates and sends a PUT.
      assertThat(logAppender.getLog(1))
            .matches("^127\\.0\\.0\\.1 - \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d*] \"AUTH /" + getDefaultCacheName() + "/- HOTROD/2\\.1\" OK \\d+ \\d+ \\d+$");
      assertThat(logAppender.getLog(2))
            .matches("^127\\.0\\.0\\.1 - \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d*] \"AUTH /" + getDefaultCacheName() + "/- HOTROD/2\\.1\" OK \\d+ \\d+ \\d+$");
      assertThat(logAppender.getLog(3))
            .matches("^127\\.0\\.0\\.1 user \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d*] \"PUT /" + getDefaultCacheName() + "/\\[B0x6B HOTROD/2\\.1\" OK \\d+ \\d+ \\d+$");
   }
}
