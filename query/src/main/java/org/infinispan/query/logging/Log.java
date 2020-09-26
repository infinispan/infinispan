package org.infinispan.query.logging;

import static org.jboss.logging.Logger.Level.DEBUG;
import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.INFO;
import static org.jboss.logging.Logger.Level.WARN;

import javax.transaction.Transaction;

import org.hibernate.search.util.common.SearchException;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.objectfilter.ParsingException;
import org.infinispan.search.mapper.common.EntityReference;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

//TODO [anistor] query-core and query modules share the id range!
/**
 * Log abstraction for the query module. For this module, message ids
 * ranging from 14001 to 14500 inclusively have been reserved.
 *
 * @author Galder Zamarreño
 * @author Sanne Grinovero
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
//@ValidIdRange(min = 14001, max = 14500)
public interface Log extends org.infinispan.query.core.impl.Log {

   Log CONTAINER = Logger.getMessageLogger(Log.class, LOG_ROOT + "CONTAINER");

   @Message(value = "The configured entity class %s is not indexable. Please remove it from the indexing configuration.", id = 404)
   CacheConfigurationException classNotIndexable(String className);

   @LogMessage(level = ERROR)
   @Message(value = "Could not locate key class %s", id = 14001)
   void keyClassNotFound(String keyClassName, @Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Cannot instantiate Transformer class %s", id = 14002)
   void couldNotInstantiateTransformerClass(Class<?> transformer, @Cause Exception e);

   @LogMessage(level = INFO)
   @Message(value = "Registering Query interceptor for cache %s", id = 14003)
   void registeringQueryInterceptor(String cacheName);

   @LogMessage(level = DEBUG)
   @Message(value = "Custom commands backend initialized backing index %s", id = 14004)
   void commandsBackendInitialized(String indexName);

//   @LogMessage(level = TRACE)
//   @Message(value = "Sent list of LuceneWork %s to node %s", id = 14005)
//   void workListRemotedTo(Object workList, Address primaryNodeAddress);
//
//   @LogMessage(level = WARN)
//   @Message(value = "Index named '%1$s' is ignoring configuration option 'directory.type' set '%2$s':" +
//         " overridden to use the Infinispan Directory", id = 14008)
//   void ignoreDirectoryProviderProperty(String indexName, String directoryOption);

   @LogMessage(level = WARN)
   @Message(value = "Indexed type '%1$s' is using a default Transformer. This is slow! Register a custom implementation using @Transformable", id = 14009)
   void typeIsUsingDefaultTransformer(Class<?> keyClass);

//   @Message(value = "An IOException happened where none where expected", id = 14010)
//   CacheException unexpectedIOException(@Cause IOException e);
//
//   @LogMessage(level = WARN)
//   @Message(value = "Some indexing work was lost because of an InterruptedException", id = 14011)
//   void interruptedWhileBufferingWork(@Cause InterruptedException e);
//
//   @LogMessage(level = DEBUG)
//   @Message(value = "Waiting for index lock was successful: '%1$s'", id = 14012)
//   void waitingForLockAcquired(boolean waitForAvailabilityInternal);

   @Message(value = "Cache named '%1$s' is being shut down. No longer accepting remote commands.", id = 14013)
   CacheException cacheIsStoppingNoCommandAllowed(String cacheName);

   @LogMessage(level = INFO)
   @Message(value = "Reindexed %1$d entities", id = 14014)
   void indexingEntitiesCompleted(long nbrOfEntities);

   @LogMessage(level = INFO)
   @Message(value = "%1$d documents indexed in %2$d ms", id = 14015)
   void indexingDocumentsCompleted(long doneCount, long elapsedMs);

   @LogMessage(level = INFO)
   @Message(value = "Purging instances of '%s' from the index", id = 14016)
   void purgingIndex(String entityType);

   @LogMessage(level = INFO)
   @Message(value = "Flushing index '%s'", id = 14017)
   void flushingIndex(String entityType);

//   @Message(value = "Error executing MassIndexer", id = 14018)
//   CacheException errorExecutingMassIndexer(@Cause Throwable cause);

   @Message(value = "Cannot run Lucene queries on a cache '%s' that does not have indexing enabled", id = 14019)
   IllegalStateException cannotRunLuceneQueriesIfNotIndexed(String cacheName);

//   @LogMessage(level = WARN)
//   @Message(value = "Autodetected a new indexed entity type in cache %s: %s. Autodetection support will be removed in Infinispan 10.0.", id = 14028)
//   void detectedUnknownIndexedEntity(String cacheName, String className);

//   @LogMessage(level = WARN)
//   @Message(value = "Found undeclared indexable types in cache %s : %s. No indexes were created for these types because autodetection is not enabled for this cache.", id = 14029)
//   void detectedUnknownIndexedEntities(String cacheName, String classNames);

//   @Message(value = "The type %s is not an indexed entity.", id = 14030)
//   IllegalArgumentException notAnIndexedEntityException(String typeName);

   @Message(value = "Unable to resume suspended transaction %s", id = 14033)
   CacheException unableToResumeSuspendedTx(Transaction transaction, @Cause Throwable cause);

   @Message(value = "Unable to suspend transaction", id = 14034)
   CacheException unableToSuspendTx(@Cause Throwable cause);

   @Message(value = "Prefix, wildcard or regexp queries cannot be fuzzy: %s", id = 14036)
   ParsingException getPrefixWildcardOrRegexpQueriesCannotBeFuzzy(String s); //todo [anistor] this should be thrown earlier at parsing time

//   @Message(value = "Invalid boolean literal '%s'", id = 14037)
//   ParsingException getInvalidBooleanLiteralException(String value);
//
//   @Message(value = "infinispan-query.jar module is in the classpath but has not been properly initialised!", id = 14038)
//   CacheException queryModuleNotInitialised();

   @Message(value = "Queries containing groups or aggregations cannot be converted to an indexed query", id = 14039)
   CacheException groupAggregationsNotSupported();

   @Message(value = "Unable to define filters, please use filters in the query string instead.", id = 14040)
   CacheException filterNotSupportedWithQueryString();

//   @Message(value = "Unable to define sort, please use sorting in the query string instead.", id = 14041)
//   CacheException sortNotSupportedWithQueryString();

   @Message(value = "Cannot find an appropriate Transformer for key type %s. Indexing only works with entries keyed " +
         "on Strings, primitives, byte[], UUID, classes that have the @Transformable annotation or classes for which " +
         "you have defined a suitable Transformer in the indexing configuration. Alternatively, see " +
         "org.infinispan.query.spi.SearchManagerImplementor.registerKeyTransformer.", id = 14043)
   CacheException noTransformerForKey(String keyClassName);

   @LogMessage(level = ERROR)
   @Message(value = "Failed to parse system property %s", id = 14044)
   void failedToParseSystemProperty(String propertyName, @Cause Exception e);

//   @LogMessage(level = DEBUG)
//   @Message(value = "Overriding org.apache.lucene.search.BooleanQuery.setMaxClauseCount to value %d to be able to deserialize a larger BooleanQuery", id = 14045)
//   void overridingBooleanQueryMaxClauseCount(int maxClauseCount);

   @LogMessage(level = INFO)
   @Message(value = "Setting org.apache.lucene.search.BooleanQuery.setMaxClauseCount from system property %s to value %d", id = 14046)
   void settingBooleanQueryMaxClauseCount(String sysPropName, int maxClauseCount);

   @LogMessage(level = WARN)
   @Message(value = "Ignoring system property %s because the value %d is smaller than the current value (%d) of org.apache.lucene.search.BooleanQuery.getMaxClauseCount()", id = 14047)
   void ignoringBooleanQueryMaxClauseCount(String sysPropName, int maxClauseCount, int currentMaxClauseCount);

//   @Message(value = "Error acquiring MassIndexer Lock", id = 14048)
//   CacheException errorAcquiringMassIndexerLock(@Cause Throwable e);
//
//   @Message(value = "Error releasing MassIndexer Lock", id = 14049)
//   CacheException errorReleasingMassIndexerLock(@Cause Throwable e);

   @Message(value = "Interrupted while waiting for completions of some batch indexing operations.", id = 14050)
   CacheException interruptedWhileWaitingForRequestCompletion(@Cause Exception cause);

   @Message(value = "%1$s entities could not be indexed. See the logs for details. First failure on entity '%2$s': %3$s", id = 14051)
   SearchException massIndexingEntityFailures(long finalFailureCount, EntityReference firstFailureEntity, String firstFailureMessage, @Cause Throwable firstFailure);

   @Message(value = "Indexing instance of entity '%s' during mass indexing", id = 14052)
   String massIndexerIndexingInstance(String entityName);

   @Message(value = "Invalid property key '%1$s`, it's not a string.", id = 14053)
   CacheException invalidPropertyKey(Object propertyKey);

   @Message(value = "Trying to execute query `%1$s`, but no type is indexed on cache.", id = 14054)
   CacheException noTypeIsIndexed(String ickle);
}
