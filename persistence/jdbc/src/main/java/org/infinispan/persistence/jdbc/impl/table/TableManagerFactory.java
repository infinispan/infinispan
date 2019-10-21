package org.infinispan.persistence.jdbc.impl.table;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Arrays;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.persistence.jdbc.DatabaseType;
import org.infinispan.persistence.jdbc.configuration.AbstractJdbcStoreConfiguration;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration;
import org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Ryan Emerson
 */
public class TableManagerFactory {

   private static final Log log = LogFactory.getLog(TableManagerFactory.class, Log.class);
   public static final String UPSERT_DISABLED = "infinispan.jdbc.upsert.disabled";
   public static final String INDEXING_DISABLED = "infinispan.jdbc.indexing.disabled";

   public static TableManager getManager(ConnectionFactory connectionFactory, JdbcStringBasedStoreConfiguration config, String cacheName) {
      DbMetaData metaData = getDbMetaData(connectionFactory, config.dialect(), config.dbMajorVersion(),
            config.dbMinorVersion(), isPropertyDisabled(config, UPSERT_DISABLED),
            isPropertyDisabled(config, INDEXING_DISABLED), !config.segmented());

      return getManager(metaData, connectionFactory, config.table(), cacheName);
   }

   public static TableManager getManager(DbMetaData metaData, ConnectionFactory connectionFactory,
                                         TableManipulationConfiguration tableConfig, String cacheName) {
      switch (metaData.getType()) {
         case DB2:
         case DB2_390:
            return new DB2TableManager(connectionFactory, tableConfig, metaData, cacheName);
         case H2:
            return new H2TableManager(connectionFactory, tableConfig, metaData, cacheName);
         case MARIA_DB:
         case MYSQL:
            return new MySQLTableManager(connectionFactory, tableConfig, metaData, cacheName);
         case ORACLE:
            return new OracleTableManager(connectionFactory, tableConfig, metaData, cacheName);
         case POSTGRES:
            return new PostgresTableManager(connectionFactory, tableConfig, metaData, cacheName);
         case SQLITE:
            return new SQLiteTableManager(connectionFactory, tableConfig, metaData, cacheName);
         case SYBASE:
            return new SybaseTableManager(connectionFactory, tableConfig, metaData, cacheName);
         case SQL_SERVER:
            return new SQLServerTableManager(connectionFactory, tableConfig, metaData, cacheName);
         default:
            return new GenericTableManager(connectionFactory, tableConfig, metaData, cacheName);
      }
   }

   private static DbMetaData getDbMetaData(ConnectionFactory connectionFactory, DatabaseType databaseType,
                                           Integer majorVersion, Integer minorVersion, boolean disableUpsert,
                                           boolean disableIndexing, boolean disableSegmented) {
      if (databaseType != null && majorVersion != null && minorVersion != null)
         return new DbMetaData(databaseType, majorVersion, minorVersion, disableUpsert, disableIndexing, disableSegmented);

      Connection connection = null;
      if (majorVersion == null || minorVersion == null) {
         try {
            // Try to retrieve major and minor simultaneously, if both aren't available then no use anyway
            connection = connectionFactory.getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            majorVersion = metaData.getDatabaseMajorVersion();
            minorVersion = metaData.getDatabaseMinorVersion();

            String version = majorVersion + "." + minorVersion;
            if (log.isDebugEnabled()) {
               log.debugf("Guessing database version as '%s'.  If this is incorrect, please specify both the correct " +
                                "major and minor version of your database using the 'databaseMajorVersion' and " +
                                "'databaseMinorVersion' attributes in your configuration.", version);
            }

            // If we already know the DatabaseType via User, then don't check
            if (databaseType != null)
               return new DbMetaData(databaseType, majorVersion, minorVersion, disableUpsert, disableIndexing, disableSegmented);
         } catch (SQLException e) {
            if (log.isDebugEnabled())
               log.debug("Unable to retrieve DB Major and Minor versions from JDBC metadata.", e);
         } finally {
            connectionFactory.releaseConnection(connection);
         }
      }

      try {
         connection = connectionFactory.getConnection();
         String dbProduct = connection.getMetaData().getDatabaseProductName();
         return new DbMetaData(guessDialect(dbProduct), majorVersion, minorVersion, disableUpsert, disableIndexing, disableSegmented);
      } catch (Exception e) {
         if (log.isDebugEnabled())
            log.debug("Unable to guess dialect from JDBC metadata.", e);
      } finally {
         connectionFactory.releaseConnection(connection);
      }

      if (log.isDebugEnabled())
         log.debug("Unable to detect database dialect using connection metadata.  Attempting to guess on driver name.");
      try {
         connection = connectionFactory.getConnection();
         String dbProduct = connectionFactory.getConnection().getMetaData().getDriverName();
         return new DbMetaData(guessDialect(dbProduct), majorVersion, minorVersion, disableUpsert, disableIndexing, disableSegmented);
      } catch (Exception e) {
         if (log.isDebugEnabled())
            log.debug("Unable to guess database dialect from JDBC driver name.", e);
      } finally {
         connectionFactory.releaseConnection(connection);
      }

      if (databaseType == null) {
         throw new CacheConfigurationException("Unable to detect database dialect from JDBC driver name or connection metadata.  Please provide this manually using the 'dialect' property in your configuration.  Supported database dialect strings are " + Arrays.toString(DatabaseType.values()));
      }

      if (log.isDebugEnabled())
         log.debugf("Guessing database dialect as '%s'.  If this is incorrect, please specify the correct dialect using the 'dialect' attribute in your configuration.  Supported database dialect strings are %s", databaseType, Arrays.toString(DatabaseType.values()));
      return new DbMetaData(databaseType, majorVersion, minorVersion, disableUpsert, disableIndexing, disableSegmented);
   }

   private static DatabaseType guessDialect(String name) {
      DatabaseType type = null;
      if (name == null)
         return null;

      name = name.toLowerCase();
      if (name.contains("mysql")) {
         type = DatabaseType.MYSQL;
      } else if (name.contains("mariadb")) {
         type = DatabaseType.MARIA_DB;
      } else if (name.contains("postgres")) {
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

   private static boolean isPropertyDisabled(AbstractJdbcStoreConfiguration config, String propertyName) {
      String property = config.properties().getProperty(propertyName);
      return property != null && Boolean.parseBoolean(property);
   }
}
