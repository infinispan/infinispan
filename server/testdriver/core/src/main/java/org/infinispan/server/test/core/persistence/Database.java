package org.infinispan.server.test.core.persistence;

import java.util.Properties;

/**
 * @author Gustavo Lira &lt;glira@redhat.com&gt;
 * @since 10.0
 **/
public abstract class Database {
   public static final String ENV_PREFIX = "database.container.env.";
   private final String type;
   protected Properties properties;

   protected Database(String type, Properties properties) {
      this.type = type;
      this.properties = properties;
   }

   public String getType() {
      return type;
   }

   public String getDataColumnType() {
      return properties.getProperty("data.column.type");
   }

   public String getTimeStampColumnType() {
      return properties.getProperty("timestamp.column.type");
   }

   public String getSegmentColumnType() {
      return properties.getProperty("segment.column.type");
   }

   public String getIdColumType() {
      return properties.getProperty("id.column.type");
   }

   public String jdbcUrl() {
      return properties.getProperty("database.jdbc.url");
   }

   public String username() {
      return properties.getProperty("database.jdbc.username");
   }

   public String password() {
      return properties.getProperty("database.jdbc.password");
   }

   public String driverClassName() {
      return properties.getProperty("database.jdbc.driver");
   }

   public String testQuery() {
      return properties.getProperty("database.test.query");
   }

   public static Database fromProperties(String type, Properties properties) {
      String mode = properties.getProperty("database.mode");
      switch (mode) {
         case "CONTAINER":
            return new ContainerDatabase(type, properties);
         case "EXTERNAL":
            return new ExternalDatabase(type, properties);
         default:
            throw new IllegalArgumentException(mode);
      }
   }

   public abstract void start();

   public abstract void stop();

   @Override
   public String toString() {
      return "Database{" +
            "type='" + type + '\'' +
            '}';
   }
}
