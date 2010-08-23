package org.infinispan.loaders.jdbc;

import java.util.Arrays;

/**
 * Supported database dialects for the Jdbc cache stores
 *
 * @author Manik Surtani
 * @since 4.1
 */
public enum DatabaseType {
   MYSQL, POSTGRES, DERBY, HSQL, H2, SQLITE,
   DB2,
   INFORMIX, INTERBASE, FIREBIRD,
   SQL_SERVER, ACCESS,
   ORACLE;
}
