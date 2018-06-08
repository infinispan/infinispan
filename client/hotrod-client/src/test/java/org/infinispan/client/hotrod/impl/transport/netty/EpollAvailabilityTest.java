package org.infinispan.client.hotrod.impl.transport.netty;

import static org.testng.AssertJUnit.assertTrue;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.infinispan.commons.test.skip.StringLogAppender;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CherryPickClassLoader;
import org.testng.annotations.Test;

@Test(testName = "client.hotrod.impl.transport.netty.EpollAvailabilityTest")
public class EpollAvailabilityTest extends AbstractInfinispanTest {
   public static final String TRANSPORT_HELPER_CLASS = "org.infinispan.client.hotrod.impl.transport.netty.TransportHelper";
   public static final String LOG_FORMAT = "%-5p (%t) [%c{1}] %m%throwable{10}%n";

   public void testEpollNotAvailable() throws Exception {
      StringLogAppender logAppender = new StringLogAppender(TRANSPORT_HELPER_CLASS,
            Level.TRACE,
            t -> t.getName().contains(EpollAvailabilityTest.class.getSimpleName()),
            PatternLayout.newBuilder().withPattern(LOG_FORMAT).build());
      logAppender.install();
      try {
         CherryPickClassLoader classLoader = new CherryPickClassLoader(
               new String[]{TRANSPORT_HELPER_CLASS},
               null,
               new String[]{"io.netty.channel.epoll.Epoll"}, this.getClass().getClassLoader()
         );
         Class.forName(TRANSPORT_HELPER_CLASS, true, classLoader);
         assertTrue(logAppender.getLog(0).contains(" is discarded"));
      } finally {
         logAppender.uninstall();
      }
   }
}
