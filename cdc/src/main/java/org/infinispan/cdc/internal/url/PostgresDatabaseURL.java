package org.infinispan.cdc.internal.url;

import java.sql.SQLException;

import org.infinispan.cdc.internal.configuration.vendor.DatabaseVendor;

/**
 * Parses the JDBC URL for Postgres databases.
 *
 * @author José Bolina
 * @since 16.0
 * @see <a href="https://jdbc.postgresql.org/documentation/use/">Postgres JDBC documentation.</a>
 */
final class PostgresDatabaseURL extends BaseJdbcURL {

   PostgresDatabaseURL(String jdbcUrl) throws SQLException {
      super(jdbcUrl);
   }

   @Override
   protected String hostProperty() {
      return "PGHOST";
   }

   @Override
   protected String portProperty() {
      return "PGPORT";
   }

   @Override
   protected String schemaProperty() {
      return "PGDBNAME";
   }

   @Override
   protected String databaseNameProperty() {
      return "currentSchema";
   }

   @Override
   public DatabaseVendor vendor() {
      return DatabaseVendor.POSTGRES;
   }
}
