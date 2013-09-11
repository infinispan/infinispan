package org.infinispan.persistence.jdbc.connectionfactory;

/**
 * Contains configuration elements for a {@link ConnectionFactory}.
 *
 * @author Mircea.Markus@jboss.com
 */
public class ConnectionFactoryConfig implements Cloneable {

   private String connectionFactoryClass;
   private String driverClass;
   private String connectionUrl;
   private String userName;
   private String password;
   private String datasourceJndiLocation;

   public ConnectionFactoryConfig(String connectionFactoryClass, String driverClass, String connectionUrl,
                                  String userName, String password) {
      this.connectionFactoryClass = connectionFactoryClass;
      this.driverClass = driverClass;
      this.connectionUrl = connectionUrl;
      this.userName = userName;
      this.password = password;
   }

   public ConnectionFactoryConfig() {
   }

   public String getDriverClass() {
      return driverClass;
   }

   public String getConnectionUrl() {
      return connectionUrl;
   }

   public String getUserName() {
      return userName;
   }

   public String getPassword() {
      return password;
   }

   public void setDriverClass(String driverClass) {
      this.driverClass = driverClass;
   }

   public void setConnectionUrl(String connectionUrl) {
      this.connectionUrl = connectionUrl;
   }

   public void setUserName(String userName) {
      this.userName = userName;
   }

   public void setPassword(String password) {
      this.password = password;
   }

   public void setConnectionFactoryClass(String connectionFactoryClass) {
      this.connectionFactoryClass = connectionFactoryClass;
   }

   public String getConnectionFactoryClass() {
      return connectionFactoryClass;
   }

   public void setDatasourceJndiLocation(String location) {
      datasourceJndiLocation = location;
   }

   public String getDatasourceJndiLocation() {
      return datasourceJndiLocation;
   }

   @Override
   public ConnectionFactoryConfig clone() {
      try {
         return (ConnectionFactoryConfig) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new IllegalStateException(e);
      }
   }

   @Override
   public String toString() {
      return "ConnectionFactoryConfig{" +
            "connectionFactoryClass='" + connectionFactoryClass + '\'' +
            ", driverClass='" + driverClass + '\'' +
            ", connectionUrl='" + connectionUrl + '\'' +
            ", userName='" + userName + '\'' +
            '}';
   }
}
