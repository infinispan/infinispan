package org.infinispan.persistence.jdbc.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.connectionfactory.SimpleConnectionFactory;

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
}
