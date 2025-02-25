package org.infinispan.cdc.internal.url;

import static org.infinispan.cdc.internal.url.DatabaseConstants.DB2_DEFAULT_PORT;
import static org.infinispan.cdc.internal.url.DatabaseConstants.MYSQL_DEFAULT_PORT;
import static org.infinispan.cdc.internal.url.DatabaseConstants.POSTGRES_DEFAULT_PORT;

import org.infinispan.cdc.internal.configuration.vendor.DatabaseVendor;

/**
 * Describes a JDBC URL.
 * <p>
 * The JDBC URL is not always possible to represent through a {@link java.net.URI} object. This interface provides
 * a lower-level access to the parameters in the URL. Since each vendor might follow a different representation,
 * each implementation might vary from other to identify the parameters.
 * </p>
 *
 * @since 15.2
 * @author José Bolina
 */
public interface DatabaseURL {

   /**
    * The database host name.
    * <p>
    * For example, `<code>jdbc:mysql://127.0.0.1:3306/example</code>` should return `<code>127.0.0.1</code>`.
    * </p>
    *
    * @return The host name of the JDBC URL.
    * @see java.net.URI#getHost()
    */
   String host();

   /**
    * The database port.
    * <p>
    * For example, `<code>jdbc:mysql://127.0.0.1:3306/example</code>` should return `<code>3306</code>`.
    * </p>
    *
    * @return The port number in String format.
    * @see java.net.URI#getPort()
    */
   String port();

   /**
    * The database provided in the JDBC URL.
    * <p>
    * For example, `<code>jdbc:mysql://127.0.0.1:3306/example</code>` should return `<code>example</code>`.
    * </p>
    *
    * @return The database identified in the JDBC URL.
    * @see java.net.URI#getPath()
    */
   String database();

   /**
    * The database vendor identified in the JDBC URL.
    *
    * @return The vendor identified in the JDBC URL.
    */
   DatabaseVendor vendor();

   /**
    * Parses the given string into a descriptive URL.
    *
    * <p>
    * Parses a string representing a JDBC URL connection to acquire more information. Currently, the parse identifies
    * only the databases identified by the {@link DatabaseVendor} enumeration.
    * </p>
    *
    * @param connectionUrl The JDBC URL
    * @return A new {@link DatabaseURL}.
    * @throws IllegalArgumentException If the database vendor is not identifiable.
    */
   static DatabaseURL create(String connectionUrl) {
      String type = DatabaseConstants.getDatabaseType(connectionUrl);
      return switch (type) {
         case "mysql" -> new DefaultDatabaseURL(connectionUrl, MYSQL_DEFAULT_PORT, DatabaseVendor.MYSQL);
         case "postgres", "postgresql" -> new DefaultDatabaseURL(connectionUrl, POSTGRES_DEFAULT_PORT, DatabaseVendor.POSTGRES);
         case "db2" -> new DefaultDatabaseURL(connectionUrl, DB2_DEFAULT_PORT, DatabaseVendor.DB2);
         case "oracle" -> new OracleDatabaseURL(connectionUrl);
         case "sqlserver" -> new SqlServerDatabaseURL(connectionUrl);
         default -> throw new IllegalArgumentException("Unknown database type: " + type);
      };
   }
}
