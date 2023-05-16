package org.infinispan.cli.connection;

import java.util.Properties;

import org.infinispan.cli.impl.SSLContextSettings;
import org.infinispan.commons.util.ServiceFinder;

public final class ConnectionFactory {
   public static Connection getConnection(Properties properties, String connectionString, SSLContextSettings sslContext) {
      for (Connector connector : ServiceFinder.load(Connector.class)) {
         Connection connection = connector.getConnection(properties, connectionString, sslContext);
         if (connection != null) {
            return connection;
         }
      }
      return null;
   }

}
