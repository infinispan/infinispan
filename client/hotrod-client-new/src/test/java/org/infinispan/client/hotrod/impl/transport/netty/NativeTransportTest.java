package org.infinispan.client.hotrod.impl.transport.netty;

import static org.testng.AssertJUnit.assertTrue;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.infinispan.commons.test.skip.OS;
import org.infinispan.commons.test.skip.SkipTestNG;
import org.infinispan.commons.test.skip.StringLogAppender;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CherryPickClassLoader;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.impl.transport.netty.NativeTransportTest")
public class NativeTransportTest extends AbstractInfinispanTest {
   public static final String NATIVE_TRANSPORT_CLASS = "org.infinispan.client.hotrod.impl.transport.netty.NativeTransport";
   public static final String LOG_FORMAT = "%-5p (%t) [%c{1}] %m%throwable{10}%n";

   public void testEpollNotAvailable() throws Exception {
      SkipTestNG.onlyOnOS(OS.LINUX);
      Thread testThread = Thread.currentThread();
      StringLogAppender logAppender = new StringLogAppender("org.infinispan.HOTROD",
            Level.TRACE,
            t -> t == testThread,
            PatternLayout.newBuilder().withPattern(LOG_FORMAT).build());
      logAppender.install();
      try {
         CherryPickClassLoader classLoader = new CherryPickClassLoader(
               new String[]{NATIVE_TRANSPORT_CLASS},
               null,
               new String[]{"io.netty.channel.epoll.Epoll"}, this.getClass().getClassLoader()
         );
         Class.forName(NATIVE_TRANSPORT_CLASS, true, classLoader);
         String firstLine = logAppender.getLog(0);
         assertTrue(firstLine, firstLine.contains("io.netty.channel.epoll.Epoll"));
      } finally {
         logAppender.uninstall();
      }
   }
}
