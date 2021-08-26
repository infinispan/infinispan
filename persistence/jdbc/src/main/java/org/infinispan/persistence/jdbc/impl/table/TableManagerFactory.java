package org.infinispan.persistence.jdbc.impl.table;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Arrays;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.persistence.jdbc.common.DatabaseType;
import org.infinispan.persistence.jdbc.common.configuration.AbstractJdbcStoreConfiguration;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.common.logging.Log;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Ryan Emerson
 */
public class TableManagerFactory {

   private static final Log log = LogFactory.getLog(TableManagerFactory.class, Log.class);
   public static final String UPSERT_DISABLED = "infinispan.jdbc.upsert.disabled";
   public static final String INDEXING_DISABLED = "infinispan.jdbc.indexing.disabled";

   public static <K, V> TableManager<K, V> getManager(InitializationContext ctx, ConnectionFactory connectionFactory,
         JdbcStringBasedStoreConfiguration config, String cacheName) {
      DbMetaData metaData = getDbMetaData(connectionFactory, config.dialect(), config.dbMajorVersion(),
            config.dbMinorVersion(), isPropertyDisabled(config, UPSERT_DISABLED),
            isPropertyDisabled(config, INDEXING_DISABLED), !config.segmented());

      return getManager(metaData, ctx, connectionFactory, config, cacheName);
   }

   public static <K, V> TableManager<K, V> getManager(DbMetaData metaData, InitializationContext ctx,
         ConnectionFactory connectionFactory, JdbcStringBasedStoreConfiguration config, String cacheName) {
      switch (metaData.getType()) {
         case DB2:
         case DB2_390:
            return new DB2TableManager(ctx, connectionFactory, config, metaData, cacheName);
         case H2:
            return new H2TableManager(ctx, connectionFactory, config, metaData, cacheName);
         case MARIA_DB:
         case MYSQL:
            return new MyTableOperations(ctx, connectionFactory, config, metaData, cacheName);
         case ORACLE:
            return new OracleTableManager(ctx, connectionFactory, config, metaData, cacheName);
         case POSTGRES:
            return new PostgresTableManager(ctx, connectionFactory, config, metaData, cacheName);
         case SQLITE:
            return new SQLiteTableManager(ctx, connectionFactory, config, metaData, cacheName);
         case SYBASE:
            return new SybaseTableManager(ctx, connectionFactory, config, metaData, cacheName);
         case SQL_SERVER:
            return new TableOperations(ctx, connectionFactory, config, metaData, cacheName);
         default:
            return new GenericTableManager(ctx, connectionFactory, config, metaData, cacheName);
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
         return new DbMetaData(DatabaseType.guessDialect(dbProduct), majorVersion, minorVersion, disableUpsert, disableIndexing, disableSegmented);
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
         return new DbMetaData(DatabaseType.guessDialect(dbProduct), majorVersion, minorVersion, disableUpsert, disableIndexing, disableSegmented);
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

   private static boolean isPropertyDisabled(AbstractJdbcStoreConfiguration config, String propertyName) {
      String property = config.properties().getProperty(propertyName);
      return property != null && Boolean.parseBoolean(property);
   }
}
