package org.infinispan.persistence.jdbc.common;

import java.util.List;

import org.infinispan.persistence.jdbc.common.impl.table.DB2SqlManager;
import org.infinispan.persistence.jdbc.common.impl.table.GenericSqlManager;
import org.infinispan.persistence.jdbc.common.impl.table.H2SqlManager;
import org.infinispan.persistence.jdbc.common.impl.table.MySQLSqlManager;
import org.infinispan.persistence.jdbc.common.impl.table.OracleSqlManager;
import org.infinispan.persistence.jdbc.common.impl.table.PostgresqlSqlManager;
import org.infinispan.persistence.jdbc.common.impl.table.SQLLiteSqlManager;
import org.infinispan.persistence.jdbc.common.impl.table.SQLServerSqlManager;
import org.infinispan.persistence.jdbc.common.impl.table.SybaseSqlManager;

public interface SqlManager {
   String getSelectStatement(List<String> keyColumns, List<String> allColumns);

   String getSelectAllStatement(List<String> allColumns);

   String getDeleteStatement(List<String> keyColumns);

   String getDeleteAllStatement();

   String getUpsertStatement(List<String> keyColumns, List<String> allColumns);

   String getSizeCommand();

   static SqlManager fromDatabaseType(DatabaseType databaseType, String tableName) {
      return fromDatabaseType(databaseType, tableName, false);
   }

   static SqlManager fromDatabaseType(DatabaseType databaseType, String tableName, boolean namedParameters) {
      switch (databaseType) {
         case DB2:
         case DB2_390:
            return new DB2SqlManager(tableName, namedParameters);
         case H2:
            return new H2SqlManager(tableName, namedParameters);
         case MARIA_DB:
         case MYSQL:
            return new MySQLSqlManager(tableName, namedParameters);
         case ORACLE:
            return new OracleSqlManager(tableName, namedParameters);
         case POSTGRES:
            return new PostgresqlSqlManager(tableName, namedParameters);
         case SQLITE:
            return new SQLLiteSqlManager(tableName, namedParameters);
         case SYBASE:
            return new SybaseSqlManager(tableName, namedParameters);
         case SQL_SERVER:
            return new SQLServerSqlManager(tableName, namedParameters);
         default:
            return new GenericSqlManager(tableName, namedParameters);
      }
   }
}
