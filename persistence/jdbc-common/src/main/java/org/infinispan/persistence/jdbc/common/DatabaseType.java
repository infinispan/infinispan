package org.infinispan.persistence.jdbc.common;

/**
 * Supported database dialects for the JDBC cache stores
 *
 * @author Manik Surtani
 * @since 4.1
 */
public enum DatabaseType {
   ACCESS,
   DB2,
   DB2_390,
   DERBY,
   FIREBIRD,
   H2,
   HSQL,
   INFORMIX,
   INTERBASE,
   MARIA_DB,
   MYSQL,
   ORACLE,
   ORACLE_XE,
   POSTGRES,
   SQLITE,
   SQL_SERVER,
   SYBASE;

   public static DatabaseType guessDialect(String name) {
      DatabaseType type = null;
      if (name == null)
         return null;

      name = name.toLowerCase();
      if (name.contains("mysql")) {
         type = DatabaseType.MYSQL;
      } else if (name.contains("mariadb")) {
         type = DatabaseType.MARIA_DB;
         //postgresqlplus example jdbc:edb://localhost:5444/edb
      } else if (name.contains("postgres") || name.contains("edb")) {
         type = DatabaseType.POSTGRES;
      } else if (name.contains("derby")) {
         type = DatabaseType.DERBY;
      } else if (name.contains("hsql") || name.contains("hypersonic")) {
         type = DatabaseType.HSQL;
      } else if (name.contains("h2")) {
         type = DatabaseType.H2;
      } else if (name.contains("sqlite")) {
         type = DatabaseType.SQLITE;
      } else if (name.contains("db2")) {
         type = DatabaseType.DB2;
      } else if (name.contains("informix")) {
         type = DatabaseType.INFORMIX;
      } else if (name.contains("interbase")) {
         type = DatabaseType.INTERBASE;
      } else if (name.contains("firebird")) {
         type = DatabaseType.FIREBIRD;
      } else if (name.contains("sqlserver") || name.contains("microsoft")) {
         type = DatabaseType.SQL_SERVER;
      } else if (name.contains("access")) {
         type = DatabaseType.ACCESS;
      } else if (name.contains("oracle")) {
         type = DatabaseType.ORACLE;
      } else if (name.contains("adaptive")) {
         type = DatabaseType.SYBASE;
      }
      return type;
   }
}
