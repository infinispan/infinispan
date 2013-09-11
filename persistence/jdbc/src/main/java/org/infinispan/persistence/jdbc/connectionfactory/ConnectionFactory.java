package org.infinispan.persistence.jdbc.connectionfactory;

import org.infinispan.persistence.CacheLoaderException;
import org.infinispan.commons.util.Util;
import org.infinispan.persistence.jdbc.configuration.ConnectionFactoryConfiguration;

import java.sql.Connection;

/**
 * Defines the functionality a connection factory should implement.
 *
 * @author Mircea.Markus@jboss.com
 */
public abstract class ConnectionFactory {

   /**
    * Constructs a {@link ConnectionFactory} based on the supplied class
    * name.
    */
   public static ConnectionFactory getConnectionFactory(String connectionFactoryClass, ClassLoader classLoader) throws CacheLoaderException {
      return (ConnectionFactory) Util.getInstance(connectionFactoryClass, classLoader);
   }

   /**
    * Constructs a {@link ConnectionFactory} based on the supplied class
    * name.
    */
   public static ConnectionFactory getConnectionFactory(Class<? extends ConnectionFactory> connectionFactoryClass) throws CacheLoaderException {
      return Util.getInstance(connectionFactoryClass);
   }

   /**
    * Starts the connection factory. A pooled factory might be create connections here.
    */
   public abstract void start(ConnectionFactoryConfiguration factoryConfiguration, ClassLoader classLoader) throws
           CacheLoaderException;

   /**
    * Closes the connection factory, including all allocated connections etc.
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
