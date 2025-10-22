package org.infinispan.server.memcached.logging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.test.skip.StringLogAppender;
import org.infinispan.server.memcached.configuration.MemcachedProtocol;
import org.infinispan.server.memcached.test.MemcachedSingleNodeTest;
import org.testng.annotations.Test;

import net.spy.memcached.ClientMode;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.auth.AuthDescriptor;
import net.spy.memcached.internal.CheckedOperationTimeoutException;

public abstract class MemcachedBaseAuthAccessLoggingTest extends MemcachedSingleNodeTest {

   private StringLogAppender logAppender;
   private String testShortName;

   @Override
   protected void setup() throws Exception {
      testShortName = TestResourceTracker.getCurrentTestShortName();

      logAppender = new StringLogAppender(MemcachedAccessLogging.log.getName(),
            Level.TRACE,
            t -> t.getName().startsWith("non-blocking-thread-" + testShortName),
            PatternLayout.newBuilder().withPattern(MemcachedAccessLoggingTest.LOG_FORMAT).build());
      logAppender.install();
      assertTrue(MemcachedAccessLogging.isEnabled());
      super.setup();
   }

   @Override
   protected void teardown() {
      logAppender.uninstall();
      super.teardown();
   }

   @Override
   protected final boolean withAuthentication() {
      return true;
   }

   protected abstract List<String> regexes();

   private MemcachedClient createUnauthenticatedClient() throws IOException {
      MemcachedProtocol protocol = getProtocol();
      ConnectionFactoryBuilder.Protocol p = protocol == MemcachedProtocol.BINARY ? ConnectionFactoryBuilder.Protocol.BINARY : ConnectionFactoryBuilder.Protocol.TEXT;
      ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder().setProtocol(p).setOpTimeout(10_000L);
      builder.setClientMode(ClientMode.Static);

      if (p == ConnectionFactoryBuilder.Protocol.BINARY) {
         builder.setAuthDescriptor(AuthDescriptor.typical(testShortName, testShortName));
      }

      return new MemcachedClient(builder.build(), Collections.singletonList(new InetSocketAddress("127.0.0.1", server.getPort())));
   }

   @Test
   public void testAuthAccessLogging() throws Exception {
      client.set("k", 0, "v").get(5, TimeUnit.SECONDS);
      MemcachedClient unauthenticated = createUnauthenticatedClient();
      assertThatThrownBy(() -> unauthenticated.set("k", 0, "value").get(500, TimeUnit.MILLISECONDS))
            .isInstanceOf(CheckedOperationTimeoutException.class);
      server.getTransport().stop();

      List<String> patterns = regexes();
      for (int i = 0; i < patterns.size(); i++) {
         String match = patterns.get(i);
         assertThat(logAppender.getLog(i)).matches(match);
      }
   }
}
