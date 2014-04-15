package org.infinispan.persistence.jdbc;

/**
 * Supported database dialects for the JDBC cache stores
 *
 * @author Manik Surtani
 * @since 7.0
 *
 * Renamed from DatabaseType in version 7.0
 *
 */
public enum Dialect {
   MYSQL, POSTGRES, DERBY, HSQL, H2, SQLITE,
   DB2, DB2_390,
   INFORMIX, INTERBASE, FIREBIRD,
   SQL_SERVER, ACCESS,
   ORACLE, SYBASE;
}
