package org.infinispan.cdc.internal.configuration.vendor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.infinispan.cdc.internal.configuration.ForeignKey;
import org.infinispan.cdc.internal.configuration.PrimaryKey;

/**
 * @since 16.0
 * @author Pedro Ruivo
 */
final class OracleDescriptor extends AbstractVendorDescriptor {
   private static final String SELECT_PRIMARY_KEY = """
         SELECT
           cols.constraint_name, cols.column_name
         FROM
           all_constraints cons, all_cons_columns cols
         WHERE
           cols.table_name = UPPER(?)
           AND cons.constraint_type = 'P'
           AND cons.constraint_name = cols.constraint_name
           AND cons.owner = cols.owner
         """;

   private static final String SELECT_FOREIGN_KEYS = """
         WITH fk_constraints AS (
            SELECT
               ac.CONSTRAINT_NAME,
               ac.OWNER,
               r_table.TABLE_NAME,
               r_table.COLUMN_NAME
            FROM
               ALL_CONSTRAINTS ac
               JOIN ALL_CONS_COLUMNS r_table
                  ON r_table.CONSTRAINT_NAME = ac.R_CONSTRAINT_NAME
                     AND r_table.OWNER = ac.R_OWNER
            WHERE
               ac.TABLE_NAME = UPPER(?)
                  AND ac.CONSTRAINT_TYPE = 'R'
                  AND ac.STATUS = 'ENABLED'
         )
         SELECT
            fk.CONSTRAINT_NAME,
            fk.TABLE_NAME,
            b_table.COLUMN_NAME,
            fk.COLUMN_NAME
         FROM
            fk_constraints fk
            JOIN ALL_CONS_COLUMNS b_table
               ON b_table.CONSTRAINT_NAME = fk.CONSTRAINT_NAME
                  AND b_table.OWNER = fk.OWNER
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
