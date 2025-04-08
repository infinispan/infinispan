package org.infinispan.cdc.internal.url;

import java.sql.SQLException;

import org.infinispan.cdc.internal.configuration.vendor.DatabaseVendor;

/**
 * Parses the JDBC URL for MSSQL databases.
 *
 * @author José Bolina
 * @since 16.0
 * @see <a href="https://learn.microsoft.com/en-us/sql/connect/jdbc/building-the-connection-url">MSSQL Documentation.</a>
 */
final class MSSQLDatabaseURL extends BaseJdbcURL {

   MSSQLDatabaseURL(String jdbcUrl) throws SQLException {
      super(jdbcUrl);
   }

   @Override
   protected String hostProperty() {
      return "serverName";
   }

   @Override
   protected String portProperty() {
      return "portNumber";
   }

   @Override
   protected String schemaProperty() {
      return "databaseName";
   }

   @Override
   protected String databaseNameProperty() {
      return "instanceName";
   }

   @Override
   public DatabaseVendor vendor() {
      return DatabaseVendor.MSSQL;
   }
}
