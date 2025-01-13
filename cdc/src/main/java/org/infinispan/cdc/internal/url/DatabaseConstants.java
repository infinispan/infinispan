package org.infinispan.cdc.internal.url;

import java.util.Objects;

final class DatabaseConstants {

   private DatabaseConstants() { }

   static final String DATABASE_DEFAULT_HOST = "localhost";
   static final String MYSQL_DEFAULT_PORT = "3306";
   static final String POSTGRES_DEFAULT_PORT = "5432";
   static final String DB2_DEFAULT_PORT = "50000";
   static final String SQLSERVER_DEFAULT_PORT = "1433";

   public static String getDatabaseType(String url) {
      Objects.requireNonNull(url, "Database URL can not be null!");
      if (!url.startsWith("jdbc"))
         throw new IllegalArgumentException("Not a JDBC URL");

      String value = url.substring(5);
      int begin = value.indexOf(':');
      if (begin < 0)
         throw new IllegalStateException("Not found type of database: " + url);

      return value.substring(0, begin);
   }
}
