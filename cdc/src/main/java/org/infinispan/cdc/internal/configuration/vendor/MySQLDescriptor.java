package org.infinispan.cdc.internal.configuration.vendor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.infinispan.cdc.internal.configuration.ForeignKey;
import org.infinispan.cdc.internal.configuration.PrimaryKey;

/**
 * @since 16.0
 * @author Tristan Tarrant
 */
final class MySQLDescriptor extends AbstractVendorDescriptor {
   private static final String SELECT_PRIMARY_KEY = """
         select constraint_name, column_name
            from
         information_schema.key_column_usage
            where
         table_name = ?
            and constraint_name in (
               select constraint_name
                  from
                     information_schema.table_constraints
                  where
               table_name = ? and constraint_type='PRIMARY KEY'
            )
         order by ordinal_position
         """;
   private static final String SELECT_FOREIGN_KEYS = """
         select constraint_name, referenced_table_name, column_name, referenced_column_name
            from
         information_schema.key_column_usage
            where
         table_name = ?
            and constraint_name in (
               select constraint_name
                  from
               information_schema.table_constraints
                  where
               table_name = ? and constraint_type='FOREIGN KEY'
            )
         order by constraint_name, ordinal_position
         """;

   @Override
   public PrimaryKey primaryKey(Connection connection, String table) throws SQLException {
      PreparedStatement ps = connection.prepareStatement(SELECT_PRIMARY_KEY);
      ps.setString(1, table);
      ps.setString(2, table);
      return extractPrimaryKey(ps.executeQuery());
   }

   @Override
   public List<ForeignKey> foreignKeys(Connection connection, String table) throws SQLException {
      PreparedStatement ps = connection.prepareStatement(SELECT_FOREIGN_KEYS);
      ps.setString(1, table);
      ps.setString(2, table);
      return extractForeignKeys(ps.executeQuery());
   }
}
