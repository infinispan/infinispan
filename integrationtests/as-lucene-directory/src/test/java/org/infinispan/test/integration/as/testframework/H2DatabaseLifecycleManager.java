package org.infinispan.test.integration.as.testframework;

import java.sql.SQLException;

import org.h2.tools.Server;
import org.jboss.arquillian.container.spi.event.container.AfterStop;
import org.jboss.arquillian.container.spi.event.container.BeforeStart;
import org.jboss.arquillian.core.api.annotation.Observes;

public class H2DatabaseLifecycleManager {

   private int startedContainers = 0;
   private Server tcpServer;

   public synchronized void startDatabase(@Observes BeforeStart event) {
      if (startedContainers==0) {
         try {
            tcpServer = Server.createTcpServer();
            tcpServer.start();
            System.out.println("H2 database started in TCP server mode");
         } catch (SQLException e) {
            e.printStackTrace();
         }
         startedContainers++;
      }
   }

   public synchronized void stopDatabase(@Observes AfterStop event) {
      startedContainers--;
      if (startedContainers==0 && tcpServer!=null) {
         tcpServer.stop();
         System.out.println("H2 database was shut down");
         tcpServer = null;
      }
   }

}
