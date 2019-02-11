package org.infinispan.persistence.jdbc.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.impl.connectionfactory.SimpleConnectionFactory;

/**
 * SimpleConnectionFactoryConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@BuiltBy(SimpleConnectionFactoryConfigurationBuilder.class)
public class SimpleConnectionFactoryConfiguration implements ConnectionFactoryConfiguration {
   private final String connectionUrl;
   private final String driverClass;
   private final String username;
   private final String password;

   SimpleConnectionFactoryConfiguration(String connectionUrl, String driverClass, String username, String password) {
      this.connectionUrl = connectionUrl;
      this.driverClass = driverClass;
      this.username = username;
      this.password = password;
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
      return SimpleConnectionFactory.class;
   }

   @Override
   public String toString() {
      return "SimpleConnectionFactoryConfiguration [connectionUrl=" + connectionUrl + ", driverClass=" + driverClass + ", username=" + username + ", password=" + password + "]";
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SimpleConnectionFactoryConfiguration that = (SimpleConnectionFactoryConfiguration) o;

      if (connectionUrl != null ? !connectionUrl.equals(that.connectionUrl) : that.connectionUrl != null) return false;
      if (driverClass != null ? !driverClass.equals(that.driverClass) : that.driverClass != null) return false;
      if (username != null ? !username.equals(that.username) : that.username != null) return false;
      return password != null ? password.equals(that.password) : that.password == null;

   }

   @Override
   public int hashCode() {
      int result = connectionUrl != null ? connectionUrl.hashCode() : 0;
      result = 31 * result + (driverClass != null ? driverClass.hashCode() : 0);
      result = 31 * result + (username != null ? username.hashCode() : 0);
      result = 31 * result + (password != null ? password.hashCode() : 0);
      return result;
   }
}
