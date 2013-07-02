package org.infinispan.loaders.jdbc;

import org.infinispan.loaders.LockSupportCacheStoreConfig;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.commons.util.TypedProperties;

import java.util.Properties;

/**
 * This is an abstract configuration class containing common elements for all JDBC cache store types.
 *
 * @author Manik Surtani
 * @version 4.1
 */
public abstract class AbstractJdbcCacheStoreConfig extends LockSupportCacheStoreConfig {

   protected ConnectionFactoryConfig connectionFactoryConfig = new ConnectionFactoryConfig();

   protected AbstractJdbcCacheStoreConfig(ConnectionFactoryConfig connectionFactoryConfig) {
      this.connectionFactoryConfig = connectionFactoryConfig;

      Properties p = this.getProperties();
      setProperty(connectionFactoryConfig.getDriverClass(), "driverClass", p);
      setProperty(connectionFactoryConfig.getConnectionUrl(), "connectionUrl", p);
      setProperty(connectionFactoryConfig.getUserName(), "userName", p);
      setProperty(connectionFactoryConfig.getPassword(), "password", p);
      setProperty(connectionFactoryConfig.getConnectionFactoryClass(), "connectionFactoryClass", p);
      setProperty(connectionFactoryConfig.getDatasourceJndiLocation(), "datasourceJndiLocation", p);
   }

   protected AbstractJdbcCacheStoreConfig() {
   }

   public void setConnectionFactoryClass(String connectionFactoryClass) {
      testImmutability("connectionFactoryConfig");
      this.connectionFactoryConfig.setConnectionFactoryClass(connectionFactoryClass);
   }

   public ConnectionFactoryConfig getConnectionFactoryConfig() {
      return connectionFactoryConfig;
   }

   /**
    * Jdbc connection string for connecting to the database. Mandatory.
    */
   public void setConnectionUrl(String connectionUrl) {
      testImmutability("connectionFactoryConfig");
      this.connectionFactoryConfig.setConnectionUrl(connectionUrl);
   }

   /**
    * Database username.
    */
   public void setUserName(String userName) {
      testImmutability("connectionFactoryConfig");
      this.connectionFactoryConfig.setUserName(userName);
   }

   public void setDatasourceJndiLocation(String location) {
      testImmutability("datasourceJndiLocation");
      this.connectionFactoryConfig.setDatasourceJndiLocation(location);
   }

   /**
    * Database username's password.
    */
   public void setPassword(String password) {
      testImmutability("connectionFactoryConfig");
      this.connectionFactoryConfig.setPassword(password);
   }

   /**
    * The name of the driver used for connecting to the database. Mandatory, will be loaded before initiating the first
    * connection.
    */
   public void setDriverClass(String driverClassName) {
      testImmutability("connectionFactoryConfig");
      this.connectionFactoryConfig.setDriverClass(driverClassName);
   }

   @Override
   public AbstractJdbcCacheStoreConfig clone() {
      AbstractJdbcCacheStoreConfig result = (AbstractJdbcCacheStoreConfig) super.clone();
      result.connectionFactoryConfig = connectionFactoryConfig.clone();
      return result;
   }

   @Override
   public String toString() {
      return "AbstractJdbcCacheStoreConfig{" +
            "connectionFactoryConfig=" + connectionFactoryConfig +
            "} " + super.toString();
   }

   protected void setProperty(String properyValue, String propertyName, Properties p) {
      if (properyValue != null) {
         try {
            p.setProperty(propertyName, properyValue);
         } catch (UnsupportedOperationException e) {
            // Most likely immutable, so let's work around that
            TypedProperties writableProperties = new TypedProperties(p);
            writableProperties.setProperty(propertyName, properyValue);
            setProperties(writableProperties);
         }
      }
   }

}
