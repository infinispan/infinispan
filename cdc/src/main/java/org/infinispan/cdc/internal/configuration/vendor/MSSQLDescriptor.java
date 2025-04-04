package org.infinispan.cdc.internal.configuration.vendor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.infinispan.cdc.internal.configuration.ForeignKey;
import org.infinispan.cdc.internal.configuration.PrimaryKey;

/**
 * @since 16.0
 * @author Ryan Emerson
 */
final class MSSQLDescriptor extends AbstractVendorDescriptor {
   private static final String SELECT_PRIMARY_KEY = """
        SELECT
           KU.table_name as TABLENAME, column_name as PRIMARYKEYCOLUMN
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
        INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
          ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY'
          AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME
          AND KU.table_name = ?
        ORDER BY
           KU.TABLE_NAME, KU.ORDINAL_POSITION
        """;
   private static final String SELECT_FOREIGN_KEYS = """
         SELECT
            Constraint_Name = C.CONSTRAINT_NAME,
            PK_Table = PK.TABLE_NAME,
            FK_Column = CU.COLUMN_NAME,
            PK_Column = PT.COLUMN_NAME
         FROM
            INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS C
         INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS FK
            ON C.CONSTRAINT_NAME = FK.CONSTRAINT_NAME
         INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS PK
            ON C.UNIQUE_CONSTRAINT_NAME = PK.CONSTRAINT_NAME
         INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE CU
            ON C.CONSTRAINT_NAME = CU.CONSTRAINT_NAME
         INNER JOIN (
                    SELECT
                        i1.TABLE_NAME,
                        i2.COLUMN_NAME
                    FROM
                        INFORMATION_SCHEMA.TABLE_CONSTRAINTS i1
                    INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE i2
                        ON i1.CONSTRAINT_NAME = i2.CONSTRAINT_NAME
                    WHERE
                        i1.CONSTRAINT_TYPE = 'PRIMARY KEY'
                   ) PT
            ON PT.TABLE_NAME = ?
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
