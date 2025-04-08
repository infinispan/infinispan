package org.infinispan.cdc.internal.configuration;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.infinispan.cdc.configuration.ChangeDataCaptureConfiguration;
import org.infinispan.cdc.configuration.ColumnConfiguration;
import org.infinispan.cdc.configuration.TableConfiguration;
import org.infinispan.cdc.internal.configuration.vendor.VendorDescriptor;
import org.infinispan.persistence.jdbc.common.configuration.ConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;

/**
 * Access the database and describe a table.
 *
 * <p>
 * The automatic resolver connects to the configured database to describe a specific table. This method utilizes the
 * connection credentials configured to the CDC user and accesses the catalog to retrieve information about the table.
 * </p>
 *
 * <p>
 * The information contains the constraints of a single table. The constraints include:
 * <ul>
 *    <li>The columns included in the primary key constraint (see: {@link PrimaryKey})</li>
 *    <li>The foreign keys included for expansion in the denormalized view (see: {@link ForeignKey})</li>
 * </ul>
 *
 * Observe that foreign key is only for a single level. That is, we only include foreign key for the configured table,
 * we do not retrieve foreign keys recursively.
 * </p>
 *
 * <p>
 * <b>Warning:</b> The implementations to describe the constraints are dependent on the database vendor. A MySQL has a
 * different query and accesses different tables than DB2. As such, the user permissions might vary.
 * </p>
 *
 * @since 16.0
 * @author José Bolina
 */
final class DatabaseAutomaticResolver {

   private static final String GLOB_SYMBOL = "*";

   private DatabaseAutomaticResolver() { }

   static CompleteConfiguration transform(String cacheName, ChangeDataCaptureConfiguration configuration) throws SQLException {
      ConnectionFactory factory = createConnectionFactory(configuration.connectionFactory());
      ConnectionParameters parameters = ConnectionParameters.create(factory);
      Table table = getTableDefinition(cacheName, configuration, parameters, factory);
      return new CompleteConfiguration(parameters, table, configuration.connectorProperties());
   }

   private static ConnectionFactory createConnectionFactory(ConnectionFactoryConfiguration cfc) {
      ConnectionFactory factory = ConnectionFactory.getConnectionFactory(cfc.connectionFactoryClass());
      factory.start(cfc, null);
      return factory;
   }

   private static Table getTableDefinition(String cacheName, ChangeDataCaptureConfiguration configuration, ConnectionParameters parameters, ConnectionFactory factory) throws SQLException {
      TableConfiguration tc = configuration.table();

      // By default, we use the cache name as the table name to reduce the configuration overhead.
      String name = cacheName;
      if (!tc.name().isBlank())
         name = tc.name();

      VendorDescriptor descriptor = parameters.url().vendor().descriptor();
      try (Connection conn = factory.getConnection()) {
         // Assert that the primary key reflects what's in the configuration.
         // We'll use the primary key to create the cache entry, so it must be unique.
         PrimaryKey pk = descriptor.primaryKey(conn, name);
         validatePrimaryKey(pk, tc);

         // We only retrieve the foreign keys relation in case the user has added some configuration.
         Collection<ForeignKey> fks = Collections.emptyList();
         if (!configuration.foreignKeys().isEmpty()) {
            fks = extractForeignKeys(descriptor.foreignKeys(conn, name), configuration.foreignKeys());
         }

         // Include the remaining columns which don't have a constraint.
         List<String> columns = tc.columns().stream().map(ColumnConfiguration::name).toList();
         return new Table(name, pk, fks, columns);
      }
   }

   private static void validatePrimaryKey(PrimaryKey pk, TableConfiguration tc) {
      if (tc.primaryKey().name() != null && !tc.primaryKey().name().isBlank() && !pk.columns().contains(tc.primaryKey().name()))
         throw new IllegalStateException(String.format("Primary key colum for table '%s' not in identified constraints %s", tc.name(), pk.columns()));
   }

   private static Collection<ForeignKey> extractForeignKeys(Collection<ForeignKey> fks, Set<String> columns) {
      if (columns.isEmpty())
         return Collections.emptyList();

      if (columns.size() == 1) {
         // In case the user configured the foreign keys as `*`, we'll include all the columns.
         String column = columns.iterator().next();
         if (GLOB_SYMBOL.equals(column))
            return fks;
      }

      List<ForeignKey> filtered = new ArrayList<>(columns.size());
      for (ForeignKey fk : fks) {
         if (fk.columns().stream().anyMatch(columns::contains))
            filtered.add(fk);
      }

      if (filtered.isEmpty())
         throw new IllegalStateException("Provided foreign keys do not exist: " + columns);

      return filtered;
   }
}
