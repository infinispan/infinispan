package org.infinispan.cdc.internal.url;

import java.util.Objects;

final class DatabaseConstants {

   private DatabaseConstants() { }

   static final String DATABASE_DEFAULT_HOST = "localhost";
   static final String MYSQL_DEFAULT_PORT = "3306";
   static final String POSTGRES_DEFAULT_PORT = "5432";
   static final String SQLSERVER_DEFAULT_PORT = "1433";
   static final String ORACLE_DEFAULT_PORT = "1521";
   static final String DB2_DEFAULT_PORT = "50000";

   /**
    * Parse the JDBC URL to retrieve the database type from the sub-protocol.
    * <p>
    * The method expects a fully formed JDBC URL, starting with `<code>jdbc:</code>` and followed by the sub-protocol.
    * For example, `<code>jdbc:mysql://127.0.0.1:1234/DB</code>`, retrieves `<code>mysql</code>` from the URL.
    * </p>
    *
    * @param url A JDBC URL.
    * @return The database type in the JDBC URL.
    * @throws IllegalArgumentException If the url is not a JDBC URL.
    * @throws IllegalStateException If the database type is not found in the sub-protocol.
    */
   static String getDatabaseType(String url) {
      Objects.requireNonNull(url, "Database URL can not be null!");
      if (!url.startsWith("jdbc"))
         throw new IllegalArgumentException("Not a JDBC URL: " + url);

      String value = url.substring(5);
      int begin = value.indexOf(':');
      if (begin < 0)
         throw new IllegalStateException("Not found type of database: " + url);

      return value.substring(0, begin);
   }
}
