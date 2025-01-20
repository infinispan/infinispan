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

public final class DatabaseAutomaticResolver {

   private static final String GLOB_SYMBOL = "*";

   private DatabaseAutomaticResolver() { }

   public static CompleteConfiguration transform(String cacheName, ChangeDataCaptureConfiguration configuration) throws SQLException {
      ConnectionFactory factory = createConnectionFactory(configuration.connectionFactory());
      ConnectionParameters parameters = ConnectionParameters.create(factory);
      Table table = getTableDefinition(cacheName, configuration, parameters, factory);
      return new CompleteConfiguration(parameters, table);
   }

   private static ConnectionFactory createConnectionFactory(ConnectionFactoryConfiguration cfc) {
      ConnectionFactory factory = ConnectionFactory.getConnectionFactory(cfc.connectionFactoryClass());
      factory.start(cfc, null);
      return factory;
   }

   private static Table getTableDefinition(String cacheName, ChangeDataCaptureConfiguration configuration, ConnectionParameters parameters, ConnectionFactory factory) throws SQLException {
      TableConfiguration tc = configuration.table();
      String name = cacheName;
      if (!tc.name().isBlank())
         name = tc.name();

      VendorDescriptor descriptor = parameters.url().vendor().descriptor();
      try (Connection conn = factory.getConnection()) {
         PrimaryKey pk = descriptor.primaryKey(conn, name);
         validatePrimaryKey(pk, tc);

         Collection<ForeignKey> fks = Collections.emptyList();
         if (!configuration.foreignKeys().isEmpty()) {
            fks = extractForeignKeys(descriptor.foreignKeys(conn, name), configuration.foreignKeys());
         }

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
