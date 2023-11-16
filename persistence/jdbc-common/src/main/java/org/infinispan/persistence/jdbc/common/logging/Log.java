package org.infinispan.persistence.jdbc.common.logging;

import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.WARN;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import javax.naming.NamingException;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.persistence.spi.PersistenceException;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
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
   Log CONFIG = Logger.getMessageLogger(Log.class, org.infinispan.util.logging.Log.LOG_ROOT + "CONFIG");
   Log PERSISTENCE = Logger.getMessageLogger(Log.class, org.infinispan.util.logging.Log.LOG_ROOT + "PERSISTENCE");

   @LogMessage(level = ERROR)
   @Message(value = "Exception while marshalling object: %s", id = 65)
   void errorMarshallingObject(@Cause Throwable ioe, Object obj);

   @LogMessage(level = ERROR)
   @Message(value = "Failed clearing cache store", id = 8001)
   void failedClearingJdbcCacheStore(@Cause Exception e);

//   @LogMessage(level = ERROR)
//   @Message(value = "I/O failure while integrating state into store", id = 8002)
//   void ioErrorIntegratingState(@Cause IOException e);

   @LogMessage(level = ERROR)
   @Message(value = "SQL failure while integrating state into store", id = 8003)
   void sqlFailureIntegratingState(@Cause SQLException e);

//   @LogMessage(level = ERROR)
//   @Message(value = "Class not found while integrating state into store", id = 8004)
//   void classNotFoundIntegratingState(@Cause ClassNotFoundException e);
//
//   @LogMessage(level = ERROR)
//   @Message(value = "I/O Error while storing string keys to database", id = 8005)
//   void ioErrorStoringKeys(@Cause IOException e);
//
//   @LogMessage(level = ERROR)
//   @Message(value = "SQL Error while storing string keys to database", id = 8006)
//   void sqlFailureStoringKeys(@Cause SQLException e);
//
//   @LogMessage(level = ERROR)
//   @Message(value = "SQL error while fetching all StoredEntries", id = 8007)
//   void sqlFailureFetchingAllStoredEntries(@Cause SQLException e);
//
//   @LogMessage(level = ERROR)
//   @Message(value = "I/O failure while marshalling bucket: %s", id = 8008)
//   void errorMarshallingBucket(@Cause IOException ioe, Object bucket);

   @LogMessage(level = ERROR)
   @Message(value = "I/O error while unmarshalling from stream", id = 8009)
   void ioErrorUnmarshalling(@Cause IOException e);

   @LogMessage(level = ERROR)
   @Message(value = "*UNEXPECTED* ClassNotFoundException.", id = 8010)
   void unexpectedClassNotFoundException(@Cause ClassNotFoundException e);

   @LogMessage(level = ERROR)
   @Message(value = "Error while creating table; used DDL statement: '%s'", id = 8011)
   void errorCreatingTable(String sql, @Cause SQLException e);

//   @LogMessage(level = ERROR)
//   @Message(value = "Sql failure while loading key: %s", id = 8014)
//   void sqlFailureLoadingKey(String keyHashCode, @Cause SQLException e);

   @Message(value = "Could not find a connection in %s under the name '%s'", id = 8015)
   IllegalStateException connectionNotFound(String where, String dataSourceName);

   @LogMessage(level = ERROR)
   @Message(value = "Could not lookup connection with datasource %s", id = 8016)
   void namingExceptionLookingUpConnection(String dataSourceName, @Cause NamingException e);

   @LogMessage(level = WARN)
   @Message(value = "Failed to close naming context.", id = 8017)
   void failedClosingNamingCtx(@Cause NamingException e);

   @Message(value = "Sql failure retrieving connection from datasource", id = 8018)
   PersistenceException sqlFailureRetrievingConnection(@Cause SQLException e);

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
   void sqlFailureStoringKey(Object lockingKey, @Cause SQLException e);

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
   void sqlFailureReadingKey(Object key, Object lockingKey, @Cause SQLException e);

   @Message(value = "Attribute '%2$s' has not been set on '%1$s'", id = 8028)
   CacheConfigurationException tableManipulationAttributeNotSet(String groupName, String name);

   @Message(value = "A ConnectionFactory has not been specified for this store", id = 8029)
   CacheConfigurationException missingConnectionFactory();

//   @Message(value = "Cannot specify a ConnectionFactory and manageConnectionFactory at the same time", id = 8030)
//   CacheConfigurationException unmanagedConnectionFactory();

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

//   @Message(value = "Error whilst removing keys in batch from the database. Keys: %s", id = 8038)
//   PersistenceException sqlFailureDeletingBatch(Iterable<Object> keys, @Cause Exception e);

   @Message(value = "The existing store was created without segmentation enabled", id = 8039)
   CacheConfigurationException existingStoreNoSegmentation();

   @Message(value = "The existing store was created with %d segments configured, but the cache is configured with %d", id = 8040)
   CacheConfigurationException existingStoreSegmentMismatch(int existing, int cache);

   @LogMessage(level = ERROR)
   @Message(value = "Error retrieving JDBC metadata", id = 8041)
   void sqlFailureMetaRetrieval(@Cause SQLException e);

   @LogMessage(level = ERROR)
   @Message(value = "SQL failure while retrieving size", id = 8042)
   void sqlFailureSize(@Cause SQLException e);

   @Message(value = "Primary key has multiple columns but no key message schema defined, which is required when there is more than one key column", id = 8043)
   CacheConfigurationException primaryKeyMultipleColumnWithoutSchema();

   @Message(value = "Multiple non key columns but no value message schema defined, which is required when there is more than one value column", id = 8044)
   CacheConfigurationException valueMultipleColumnWithoutSchema();

   @Message(value = "Primary key %s was not found in the key schema %s", id = 8045)
   CacheConfigurationException keyNotInSchema(String primaryKeyName, String schemaName);

   @Message(value = "Additional value columns %s found that were not part of the schema, make sure the columns returned match the value schema %s", id = 8046)
   CacheConfigurationException valueNotInSchema(List<String> columnNames, String schemaName);

   @Message(value = "Schema not found for : %s", id = 8047)
   CacheConfigurationException schemaNotFound(String schemaName);

   @Message(value = "Key cannot be embedded when the value schema %s is an enum", id = 8048)
   CacheConfigurationException keyCannotEmbedWithEnum(String schemaName);

   @Message(value = "Repeated fields are not supported, found %s in schema %s", id = 8049)
   CacheConfigurationException repeatedFieldsNotSupported(String fieldName, String schemaName);

   @Message(value = "Duplicate name %s found for nested schema: %s", id = 8050)
   CacheConfigurationException duplicateFieldInSchema(String fieldName, String schemaName);

   @Message(value = "Schema contained a field %s that is required but wasn't found in the query for schema %s", id = 8051)
   CacheConfigurationException requiredSchemaFieldNotPresent(String fieldName, String schemaName);

   @Message(value = "Primary key %s was found in the value schema %s but embedded key was not true", id = 8052)
   CacheConfigurationException primaryKeyPresentButNotEmbedded(String fieldName, String schemaName);

   @Message(value = "Delete and select queries do not have matching arguments. Delete was %s and select was %s", id = 8053)
   CacheConfigurationException deleteAndSelectQueryMismatchArguments(List<String> deleteParams, List<String> selectParams);

   @Message(value = "Named parameter %s in upsert statement [%s] is not available in columns from selectAll statement [%s]", id = 8054)
   CacheConfigurationException deleteAndSelectQueryMismatchArguments(String paramName, String upsertStatement,
         String selectStatement);

   @Message(value = "No parameters are allowed for select all statement %s", id = 8055)
   CacheConfigurationException selectAllCannotHaveParameters(String selectAllStatement);

   @Message(value = "No parameters are allowed for delete all statement %s", id = 8056)
   CacheConfigurationException deleteAllCannotHaveParameters(String selectAllStatement);

   @Message(value = "No parameters are allowed for sizer statement %s", id = 8057)
   CacheConfigurationException sizeCannotHaveParameters(String selectAllStatement);

   @Message(value = "Not all key columns %s were returned from select all statement %s", id = 8058)
   CacheConfigurationException keyColumnsNotReturnedFromSelectAll(String keyColumns, String selectAllStatement);

   @Message(value = "Select parameter %s is not returned from select all statement %s, select statement is %s", id = 8059)
   CacheConfigurationException namedParamNotReturnedFromSelect(String paramName, String selectAllStatement, String selectStatement);

   @Message(value = "Non-terminated named parameter declaration at position %d in statement: %s", id = 8060)
   CacheConfigurationException nonTerminatedNamedParamInSql(int position, String sqlStatement);

   @Message(value = "Invalid character %s at position %d in statement: %s", id = 8061)
   CacheConfigurationException invalidCharacterInSql(char character, int position, String sqlStatement);

   @Message(value = "Unnamed parameters are not allowed, found one at %d in statement %s", id = 8062)
   CacheConfigurationException unnamedParametersNotAllowed(int position, String sqlStatement);

   @Message(value = "Provided table name %s is not in form of (<SCHEMA>.)<TABLE-NAME> where SCHEMA is optional", id = 8063)
   CacheConfigurationException tableNotInCorrectFormat(String tableName);

   @Message(value = "No primary keys found for table %s, check case sensitivity", id = 8064)
   CacheConfigurationException noPrimaryKeysFoundForTable(String tableName);

   @Message(value = "No column found that wasn't a primary key for table: %s", id = 8065)
   CacheConfigurationException noValueColumnForTable(String tableName);

   @Message(value = "Unable to detect database dialect from JDBC driver name or connection metadata.  Please provide this manually using the 'dialect' property in your configuration.  Supported database dialect strings are %s", id = 8066)
   CacheConfigurationException unableToDetectDialect(String supportedDialects);

   @Message(value = "The size, select and select all attributes must be set for a query store", id = 8067)
   CacheConfigurationException requiredStatementsForQueryStoreLoader();

   @Message(value = "The delete, delete all and upsert attributes must be set for a query store that allows writes", id = 8068)
   CacheConfigurationException requiredStatementsForQueryStoreWriter();

   @Message(value = "Key columns are required for QueryStore", id = 8069)
   CacheConfigurationException keyColumnsRequired();

   @Message(value = "Message name must not be null if embedded key is true", id = 8070)
   CacheConfigurationException messageNameRequiredIfEmbeddedKey();

   @Message(value = "Table name must be non null", id = 8071)
   CacheConfigurationException tableNameMissing();

   @LogMessage(level = WARN)
   @Message(value = "There was no JDBC metadata present in table %s, unable to confirm if segments are properly configured! Segments are assumed to be properly configured.", id = 8072)
   void sqlMetadataNotPresent(String tableName);
}
