package org.infinispan.persistence.jdbc.logging;

import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.WARN;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import javax.naming.NamingException;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.persistence.spi.PersistenceException;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Log abstraction for the JDBC cache store. For this module, message ids
 * ranging from 8001 to 9000 inclusively have been reserved.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {
   @LogMessage(level = ERROR)
   @Message(value = "Exception while marshalling object: %s", id = 65)
   void errorMarshallingObject(@Cause Throwable ioe, Object obj);

   @LogMessage(level = ERROR)
   @Message(value = "Failed clearing cache store", id = 8001)
   void failedClearingJdbcCacheStore(@Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "I/O failure while integrating state into store", id = 8002)
   void ioErrorIntegratingState(@Cause IOException e);

   @LogMessage(level = ERROR)
   @Message(value = "SQL failure while integrating state into store", id = 8003)
   void sqlFailureIntegratingState(@Cause SQLException e);

   @LogMessage(level = ERROR)
   @Message(value = "Class not found while integrating state into store", id = 8004)
   void classNotFoundIntegratingState(@Cause ClassNotFoundException e);

   @LogMessage(level = ERROR)
   @Message(value = "I/O Error while storing string keys to database", id = 8005)
   void ioErrorStoringKeys(@Cause IOException e);

   @LogMessage(level = ERROR)
   @Message(value = "SQL Error while storing string keys to database", id = 8006)
   void sqlFailureStoringKeys(@Cause SQLException e);

   @LogMessage(level = ERROR)
   @Message(value = "SQL error while fetching all StoredEntries", id = 8007)
   void sqlFailureFetchingAllStoredEntries(@Cause SQLException e);

   @LogMessage(level = ERROR)
   @Message(value = "I/O failure while marshalling bucket: %s", id = 8008)
   void errorMarshallingBucket(@Cause IOException ioe, Object bucket);

   @LogMessage(level = ERROR)
   @Message(value = "I/O error while unmarshalling from stream", id = 8009)
   void ioErrorUnmarshalling(@Cause IOException e);

   @LogMessage(level = ERROR)
   @Message(value = "*UNEXPECTED* ClassNotFoundException. This should not happen as Bucket class exists", id = 8010)
   void unexpectedClassNotFoundException(@Cause ClassNotFoundException e);

   @LogMessage(level = ERROR)
   @Message(value = "Error while creating table; used DDL statement: '%s'", id = 8011)
   void errorCreatingTable(String sql, @Cause SQLException e);

   @LogMessage(level = ERROR)
   @Message(value = "Sql failure while loading key: %s", id = 8014)
   void sqlFailureLoadingKey(String keyHashCode, @Cause SQLException e);

   @LogMessage(level = ERROR)
   @Message(value = "Could not find a connection in jndi under the name '%s'", id = 8015)
   void connectionInJndiNotFound(String dataSourceName);

   @LogMessage(level = ERROR)
   @Message(value = "Could not lookup connection with datasource %s", id = 8016)
   void namingExceptionLookingUpConnection(String dataSourceName, @Cause NamingException e);

   @LogMessage(level = WARN)
   @Message(value = "Failed to close naming context.", id = 8017)
   void failedClosingNamingCtx(@Cause NamingException e);

   @LogMessage(level = ERROR)
   @Message(value = "Sql failure retrieving connection from datasource", id = 8018)
   void sqlFailureRetrievingConnection(@Cause SQLException e);

   @LogMessage(level = ERROR)
   @Message(value = "Issues while closing connection %s", id = 8019)
   void sqlFailureClosingConnection(Connection conn, @Cause SQLException e);

//   @LogMessage(level = ERROR)
//   @Message(value = "Error while instatianting JDBC driver: '%s'", id = 8020)
//   void errorInstantiatingJdbcDriver(String driverClass, @Cause Exception e);

//   @LogMessage(level = WARN)
//   @Message(value = "Could not destroy C3P0 connection pool: %s", id = 8021)
//   void couldNotDestroyC3p0ConnectionPool(String pooledDataSource, @Cause SQLException e);

   @LogMessage(level = WARN)
   @Message(value = "Unexpected sql failure", id = 8022)
   void sqlFailureUnexpected(@Cause SQLException e);

   @LogMessage(level = WARN)
   @Message(value = "Failure while closing the connection to the database", id = 8023)
   void failureClosingConnection(@Cause SQLException e);

   @LogMessage(level = ERROR)
   @Message(value = "Error while storing string key to database; key: '%s'", id = 8024)
   void sqlFailureStoringKey(String lockingKey, @Cause SQLException e);

   @LogMessage(level = ERROR)
   @Message(value = "Error while removing string keys from database", id = 8025)
   void sqlFailureRemovingKeys(@Cause SQLException e);

   @LogMessage(level = ERROR)
   @Message(value = "In order for JdbcStringBasedStore to support %s, " +
         "the Key2StringMapper needs to implement TwoWayKey2StringMapper. " +
         "You should either make %s implement TwoWayKey2StringMapper or disable the sql. " +
         "See [https://jira.jboss.org/browse/ISPN-579] for more details.", id = 8026)
   void invalidKey2StringMapper(String where, String className);

   @LogMessage(level = ERROR)
   @Message(value = "SQL error while fetching stored entry with key: %s, lockingKey: %s", id = 8027)
   void sqlFailureReadingKey(Object key, String lockingKey, @Cause SQLException e);

   @Message(value = "Attribute '%s' has not been set", id = 8028)
   CacheConfigurationException tableManipulationAttributeNotSet(String name);

   @Message(value = "A ConnectionFactory has not been specified for this store", id = 8029)
   CacheConfigurationException missingConnectionFactory();

   @Message(value = "Cannot specify a ConnectionFactory and manageConnectionFactory at the same time", id = 8030)
   CacheConfigurationException unmanagedConnectionFactory();

   @LogMessage(level = ERROR)
   @Message(value = "Error committing JDBC transaction", id = 8031)
   void sqlFailureTxCommit(@Cause SQLException e);

   @LogMessage(level = ERROR)
   @Message(value = "Error during rollback of JDBC transaction", id = 8032)
   void sqlFailureTxRollback(@Cause SQLException e);

   @Message(value = "Exception encountered when preparing JDBC store Tx", id = 8033)
   PersistenceException prepareTxFailure(@Cause Throwable e);

//   @LogMessage(level = ERROR)
//   @Message(value = "Error when creating Hikari connection pool", id = 8034)
//   void errorCreatingHikariCP(@Cause Exception e);

//   @LogMessage(level = ERROR)
//   @Message(value = "Error loading HikariCP properties file, only the properties set in %s will be loaded", id = 8035)
//   void errorLoadingHikariCPProperties(String name);

   @LogMessage(level = WARN)
   @Message(value = "Unable to notify the PurgeListener of expired cache entries as the configured key2StringMapper " +
         "does not implement %s", id = 8036)
   void twoWayKey2StringMapperIsMissing(String className);

   @Message(value = "Error while writing entries in batch to the database:", id = 8037)
   PersistenceException sqlFailureWritingBatch(@Cause Throwable e);

   @Message(value = "Error whilst removing keys in batch from the database. Keys: %s", id = 8038)
   PersistenceException sqlFailureDeletingBatch(Iterable<Object> keys, @Cause Exception e);
}
