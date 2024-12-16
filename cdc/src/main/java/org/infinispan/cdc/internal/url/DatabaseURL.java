package org.infinispan.cdc.internal.url;

import java.sql.SQLException;

import org.infinispan.cdc.internal.configuration.vendor.DatabaseVendor;

/**
 * Describes a JDBC URL.
 * <p>
 * The JDBC URL is not always possible to represent through a {@link java.net.URI} object. This interface provides
 * a lower-level access to the parameters in the URL. Since each vendor might follow a different representation,
 * each implementation might vary from other to identify the parameters.
 * </p>
 *
 * @since 16.0
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
   String schema();

   /**
    * The name of the database to connect to.
    * <p>
    * The database name does not apply to every vendor. This property might return <code>null</code>.
    * </p>
    *
    * @return The name of the database.
    */
   String databaseName();

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
   static DatabaseURL create(String connectionUrl) throws SQLException {
      String type = DatabaseConstants.getDatabaseType(connectionUrl);
      return switch (type) {
         case "mysql" -> new MySQLDatabaseURL(connectionUrl);
         case "postgres", "postgresql" -> new PostgresDatabaseURL(connectionUrl);
         case "sqlserver" -> new MSSQLDatabaseURL(connectionUrl);
         case "db2", "oracle" -> throw new UnsupportedOperationException("Database " + type + " is not supported yet");
         default -> throw new IllegalArgumentException("Unknown database type: " + type);
      };
   }
}
