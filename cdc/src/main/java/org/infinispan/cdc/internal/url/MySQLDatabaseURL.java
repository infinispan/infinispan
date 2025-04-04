package org.infinispan.cdc.internal.url;

import java.sql.SQLException;

import org.infinispan.cdc.internal.configuration.vendor.DatabaseVendor;

/**
 * Parses the JDBC URL for MySQL databases.
 *
 * @author José Bolina
 * @since 16.0
 * @see <a href="https://dev.mysql.com/doc/connector-j/en/connector-j-reference-jdbc-url-format.html">MySQL documetation.</a>
 */
final class MySQLDatabaseURL extends BaseJdbcURL {

   MySQLDatabaseURL(String jdbcUrl) throws SQLException {
      super(jdbcUrl);
   }

   @Override
   protected String hostProperty() {
      return "host";
   }

   @Override
   protected String portProperty() {
      return "port";
   }

   @Override
   protected String schemaProperty() {
      return "dbname";
   }

   @Override
   protected String databaseNameProperty() {
      return null;
   }

   @Override
   public DatabaseVendor vendor() {
      return DatabaseVendor.MYSQL;
   }
}
