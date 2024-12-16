package org.infinispan.cdc.internal.configuration;

import java.sql.SQLException;
import java.util.Properties;

import org.infinispan.cdc.configuration.ChangeDataCaptureConfiguration;

/**
 * Internal configuration object for change data capture.
 *
 * <p>
 * This configuration contains all the user facing configuration and the automatically resolved parameters. The automatic
 * arguments include the constraints, foreign keys, and primary key for a single table.
 * </p>
 *
 * @param connection Parameters to establish a connection with the database.
 * @param table Information of the table to capture the change events.
 * @since 16.0
 */
public record CompleteConfiguration(ConnectionParameters connection, Table table, Properties connectorProperties) {

   public static CompleteConfiguration create(String cacheName, ChangeDataCaptureConfiguration configuration) throws SQLException {
      return DatabaseAutomaticResolver.transform(cacheName, configuration);
   }
}
