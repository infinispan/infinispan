package org.infinispan.cli.connection;

import javax.net.ssl.SSLContext;

import org.infinispan.commons.util.ServiceFinder;

public final class ConnectionFactory {
   public static Connection getConnection(String connectionString, SSLContext sslContext) {
      for (Connector connector : ServiceFinder.load(Connector.class)) {
         Connection connection = connector.getConnection(connectionString, sslContext);
         if (connection != null) {
            return connection;
         }
      }
      return null;
   }

}
