package org.infinispan.loaders.jdbc.connectionfactory;

import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.util.Util;

import java.sql.Connection;

/**
 * Defines the functionality a connection factory should implement.
 *
 * @author Mircea.Markus@jboss.com
 */
public abstract class ConnectionFactory {

   /**
    * Constructs a {@link org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory} based on the supplied class
    * name.
    */
   public static ConnectionFactory getConnectionFactory(String connectionFactoryClass) throws CacheLoaderException {
      try {
         return (ConnectionFactory) Util.getInstance(connectionFactoryClass);
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }
   }

   /**
    * Starts the connection factory. A pooled factory might be create connections here.
    */
   public abstract void start(ConnectionFactoryConfig config) throws CacheLoaderException;

   /**
    * Closes the connection factory, including all alocated connections etc.
    */
   public abstract void stop();

   /**
    * Fetches a connection from the factory.
    */
   public abstract Connection getConnection() throws CacheLoaderException;

   /**
    * Destroys a connection. Important: null might be passed in, as an valid argument.
    */
   public abstract void releaseConnection(Connection conn);
}
