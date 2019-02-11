package org.infinispan.persistence.jdbc.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.impl.connectionfactory.PooledConnectionFactory;

@BuiltBy(PooledConnectionFactoryConfigurationBuilder.class)
public class PooledConnectionFactoryConfiguration implements ConnectionFactoryConfiguration {
   private final String propertyFile;
   private final String connectionUrl;
   private final String driverClass;
   private final String username;
   private final String password;

   protected PooledConnectionFactoryConfiguration(String propertyFile, String connectionUrl, String driverClass,
                                                  String username, String password) {
      this.propertyFile = propertyFile;
      this.connectionUrl = connectionUrl;
      this.driverClass = driverClass;
      this.username = username;
      this.password = password;
   }

   public String propertyFile() {
      return propertyFile;
   }

   public String connectionUrl() {
      return connectionUrl;
   }

   public String driverClass() {
      return driverClass;
   }

   public String username() {
      return username;
   }

   public String password() {
      return password;
   }

   @Override
   public Class<? extends ConnectionFactory> connectionFactoryClass() {
      return PooledConnectionFactory.class;
   }

   @Override
   public String toString() {
      return "PooledConnectionFactoryConfiguration [propertyFile=" + propertyFile + ", connectionUrl=" +
            connectionUrl + ", driverClass=" + driverClass + ", username=" + username + ", password=" + password + "]";
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PooledConnectionFactoryConfiguration that = (PooledConnectionFactoryConfiguration) o;

      if (propertyFile != null ? !propertyFile.equals(that.propertyFile) : that.propertyFile != null) return false;
      if (connectionUrl != null ? !connectionUrl.equals(that.connectionUrl) : that.connectionUrl != null) return false;
      if (driverClass != null ? !driverClass.equals(that.driverClass) : that.driverClass != null) return false;
      if (username != null ? !username.equals(that.username) : that.username != null) return false;
      return password != null ? password.equals(that.password) : that.password == null;

   }

   @Override
   public int hashCode() {
      int result = propertyFile != null ? propertyFile.hashCode() : 0;
      result = 31 * result + (connectionUrl != null ? connectionUrl.hashCode() : 0);
      result = 31 * result + (driverClass != null ? driverClass.hashCode() : 0);
      result = 31 * result + (username != null ? username.hashCode() : 0);
      result = 31 * result + (password != null ? password.hashCode() : 0);
      return result;
   }
}
