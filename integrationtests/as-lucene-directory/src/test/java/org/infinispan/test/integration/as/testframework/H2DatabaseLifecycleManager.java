package org.infinispan.test.integration.as.testframework;

import java.sql.SQLException;

import org.h2.tools.Server;
import org.infinispan.commons.test.ThreadLeakChecker;
import org.jboss.arquillian.core.api.annotation.Observes;
import org.jboss.arquillian.test.spi.event.suite.AfterClass;
import org.jboss.arquillian.test.spi.event.suite.AfterSuite;
import org.jboss.arquillian.test.spi.event.suite.BeforeSuite;

public class H2DatabaseLifecycleManager {

   private int startedContainers = 0;
   private Server tcpServer;

   public synchronized void startDatabase(@Observes BeforeSuite event) {
      startedContainers++;
      if (startedContainers == 1) {
         try {
            tcpServer = Server.createTcpServer();
            tcpServer.start();
            System.out.println("H2 database started in TCP server mode");
         } catch (SQLException e) {
            e.printStackTrace();
         }
      }
   }

   public void ignoreThreads(@Observes AfterClass event) {
      // The AfterSuite and AfterStop events are processed after the thread leak check in JUnitTestListener.testRunFinished
      // even though the BeforeStart event was processed after JUnitTestListener.testRunStarted
      ThreadLeakChecker.ignoreThreadsContaining("H2 TCP");
   }

   public synchronized void stopDatabase(@Observes AfterSuite event) {
      startedContainers--;
      if (startedContainers == 0 && tcpServer != null) {
         tcpServer.stop();
         System.out.println("H2 database was shut down");
         tcpServer = null;
      }
   }

}
