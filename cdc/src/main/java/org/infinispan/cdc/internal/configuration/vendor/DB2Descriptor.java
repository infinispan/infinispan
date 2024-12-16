package org.infinispan.cdc.internal.configuration.vendor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.infinispan.cdc.internal.configuration.ForeignKey;
import org.infinispan.cdc.internal.configuration.PrimaryKey;

/**
 * @since 16.0
 * @author José Bolina
 */
final class DB2Descriptor extends AbstractVendorDescriptor {
   private static final String SELECT_PRIMARY_KEY = """
         select
            const.constname as pk_name,
            key.colname as column_name
         from
            syscat.tables tab
         inner join
            syscat.tabconst const on
               const.tabschema = tab.tabschema
                  and const.tabname = tab.tabname
                  and const.type = 'P'
         inner join
            syscat.keycoluse key on
               const.tabschema = key.tabschema
                  and const.tabname = key.tabname
                  and const.constname = key.constname
         where
            tab.type = 'T'
               and tab.tabschema not like 'SYS%'
               and tab.tabname = UPPER(?)
         """;

   private static final String SELECT_FOREIGN_KEYS = """
         select
            ref.constname as fk_constraint_name,
            ref.reftabname as foreign_table,
            ref.fk_colnames as fk_local_name,
            ref.pk_colnames as fk_ref_column
         from
            syscat.references ref
         where
            ref.tabname = UPPER(?)
         """;

   @Override
   public PrimaryKey primaryKey(Connection connection, String table) throws SQLException {
      PreparedStatement ps = connection.prepareStatement(SELECT_PRIMARY_KEY);
      ps.setString(1, table);
      return extractPrimaryKey(ps.executeQuery());
   }

   @Override
   public List<ForeignKey> foreignKeys(Connection connection, String table) throws SQLException {
      PreparedStatement ps = connection.prepareStatement(SELECT_FOREIGN_KEYS);
      ps.setString(1, table);
      return extractForeignKeys(ps.executeQuery());
   }
}
