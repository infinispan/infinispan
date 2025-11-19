package org.infinispan.query.core.impl;

import static org.jboss.logging.Logger.Level.DEBUG;
import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.INFO;
import static org.jboss.logging.Logger.Level.WARN;

import java.lang.invoke.MethodHandles;

import org.hibernate.search.engine.environment.classpath.spi.ClassLoadingException;
import org.hibernate.search.util.common.SearchException;
import org.hibernate.search.util.common.logging.impl.ClassFormatter;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.concurrent.CacheBackpressureFullException;
import org.infinispan.partitionhandling.AvailabilityException;
import org.infinispan.query.objectfilter.ParsingException;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.FormatWith;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.ValidIdRange;

import jakarta.transaction.Transaction;

/**
 * Log abstraction for the query module.
 *
 * @author anistor@redhat.com
 * @since 10.1
 */
@MessageLogger(projectCode = "ISPN")
@ValidIdRange(min = 14001, max = 14800)
public interface Log extends BasicLogger {

   String LOG_ROOT = "org.infinispan.";
   Log CONTAINER = Logger.getMessageLogger(MethodHandles.lookup(), Log.class, LOG_ROOT + "CONTAINER");

   static Log getLog(Class<?> clazz) {
      return Logger.getMessageLogger(MethodHandles.lookup(), Log.class, clazz.getName());
   }

   @LogMessage(level = ERROR)
   @Message(value = "Could not locate key class %s", id = 14001)
   void keyClassNotFound(String keyClassName, @Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Cannot instantiate Transformer class %s", id = 14002)
   void couldNotInstantiateTransformerClass(Class<?> transformer, @Cause Exception e);

//   @LogMessage(level = INFO)
//   @Message(value = "Registering Query interceptor for cache %s", id = 14003)
//   void registeringQueryInterceptor(String cacheName);

//   @LogMessage(level = DEBUG)
//   @Message(value = "Custom commands backend initialized backing index %s", id = 14004)
//   void commandsBackendInitialized(String indexName);

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

//   @Message(value = "Cache named '%1$s' is being shut down. No longer accepting remote commands.", id = 14013)
//   CacheException cacheIsStoppingNoCommandAllowed(String cacheName);

   @LogMessage(level = INFO)
   @Message(value = "Reindexed %1$d entities in %2$d ms", id = 14014)
   void indexingEntitiesCompleted(long nbrOfEntities, long elapsedMs);

   @LogMessage(level = DEBUG)
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

   @Message(value = "Queries containing grouping and aggregation functions must use projections.", id = 14021)
   ParsingException groupingAndAggregationQueriesMustUseProjections();

   @Message(value = "Cannot have aggregate functions in GROUP BY clause", id = 14022)
   IllegalStateException cannotHaveAggregationsInGroupByClause();

   @Message(value = "Using the multi-valued property path '%s' in the GROUP BY clause is not currently supported", id = 14023)
   ParsingException multivaluedPropertyCannotBeUsedInGroupBy(String propertyPath);

   @Message(value = "The property path '%s' cannot be used in the ORDER BY clause because it is multi-valued", id = 14024)
   ParsingException multivaluedPropertyCannotBeUsedInOrderBy(String propertyPath);

   @Message(value = "The query must not use grouping or aggregation", id = 14025)
   IllegalStateException queryMustNotUseGroupingOrAggregation();

   @Message(value = "The expression '%s' must be part of an aggregate function or it should be included in the GROUP BY clause", id = 14026)
   ParsingException expressionMustBePartOfAggregateFunctionOrShouldBeIncludedInGroupByClause(String propertyPath);

   @Message(value = "The property path '%s' cannot be projected because it is multi-valued", id = 14027)
   ParsingException multivaluedPropertyCannotBeProjected(String propertyPath);

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

//   @Message(value = "Queries containing groups or aggregations cannot be converted to an indexed query", id = 14039)
//   CacheException groupAggregationsNotSupported();

//   @Message(value = "Unable to define filters, please use filters in the query string instead.", id = 14040)
//   CacheException filterNotSupportedWithQueryString();

//   @Message(value = "Unable to define sort, please use sorting in the query string instead.", id = 14041)
//   CacheException sortNotSupportedWithQueryString();

   @Message(value = "Cannot execute query: cluster is operating in degraded mode and partition handling configuration doesn't allow reads and writes.", id = 14042)
   AvailabilityException partitionDegraded();

   @Message(value = "Cannot find an appropriate Transformer for key type %s. Indexing only works with entries keyed " +
         "on Strings, primitives, byte[], UUID, classes that have the @Transformable annotation or classes for which " +
         "you have defined a suitable Transformer in the indexing configuration. Alternatively, see " +
         "org.infinispan.query.spi.SearchManagerImplementor.registerKeyTransformer.", id = 14043)
   CacheException noTransformerForKey(String keyClassName);

//   @LogMessage(level = ERROR)
//   @Message(value = "Failed to parse system property %s", id = 14044)
//   void failedToParseSystemProperty(String propertyName, @Cause Exception e);

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
   String massIndexingEntityFailures(long finalFailureCount, Object firstFailureEntity, String firstFailureMessage);

   @Message(value = "Indexing instance of entity '%s' during mass indexing", id = 14052)
   String massIndexerIndexingInstance(String entityName);

//   @Message(value = "Invalid property key '%1$s`, it's not a string.", id = 14053)
//   CacheException invalidPropertyKey(Object propertyKey);

   @Message(value = "Trying to execute query `%1$s`, but no type is indexed on cache.", id = 14054)
   CacheException noTypeIsIndexed(String ickle);

   @Message(value = "Cannot index entry since the search mapping failed to initialize.", id = 14055)
   CacheException searchMappingUnavailable();

   @Message(value = "Only DELETE statements are supported by executeStatement", id = 14056)
   CacheException unsupportedStatement();

   @Message(value = "DELETE statements cannot use paging (firstResult/maxResults)", id = 14057)
   CacheException deleteStatementsCannotUsePaging();

   @Message(value = "Projections are not supported with entryIterator()", id = 14058)
   CacheException entryIteratorDoesNotAllowProjections();

   @LogMessage(level = WARN)
   @Message(value = "The indexing engine is restarting, index updates will be skipped for the current data changes.", id = 14059)
   void mappingIsRestarting();

   @LogMessage(level = INFO)
   @Message(value = "We're getting some errors from Hibernate Search or Lucene while we compute the index count/size for statistics."
         + " There is probably a concurrent reindexing ongoing.", id = 14060)
   void concurrentReindexingOnGetStatistics(@Cause Throwable cause);

   @Message(value = "Failed to load declared indexed class '%s'", id = 14061)
   CacheConfigurationException cannotLoadIndexedClass(String name, @Cause Throwable t);

   @LogMessage(level = DEBUG)
   @Message(value = "Search engine is reloaded before the reindexing.", id = 14062)
   void preIndexingReloading();

   @LogMessage(level = INFO)
   @Message(value = "Reindexing starting.", id = 14063)
   void indexingStarting();

   @Message(value = "Multiple knn predicates are not supported at the moment.", id = 14064)
   ParsingException multipleKnnPredicates();

   @Message(value = "Boolean predicates containing knn predicates are not supported at the moment.", id = 14065)
   ParsingException booleanKnnPredicates();

   @LogMessage(level = WARN)
   @Message(value = "Failed to purge index for segments %s", id = 14066)
   void failedToPurgeIndexForSegments(@Cause Throwable cause, IntSet removedSegments);

   @Message(value = "Hibernate Search updates are not keeping up. Look into increasing index writer queue and/or thread pool sizes.", id = 14067)
   CacheBackpressureFullException hibernateSearchBackpressure();

   @LogMessage(level = INFO)
   @Message(value = "Lucene version: %s", id = 14068)
   void luceneBackendVersion(String version);

   @Message(id = 14501, value = "Exception while retrieving the type model for '%1$s'.")
   SearchException errorRetrievingTypeModel(@FormatWith(ClassFormatter.class) Class<?> clazz, @Cause Exception cause);

   @Message(id = 14502, value = "Multiple entity types configured with the same name '%1$s': '%2$s', '%3$s'")
   SearchException multipleEntityTypesWithSameName(String entityName, Class<?> previousType, Class<?> type);

   @Message(id = 14503, value = "Infinispan Search Mapper does not support named types. The type with name '%1$s' does not exist.")
   SearchException namedTypesNotSupported(String name);

   @Message(id = 14504, value = "Unable to load class [%1$s]")
   ClassLoadingException unableToLoadTheClass(String className, @Cause Throwable cause);

   @Message(id = 14505, value = "Unknown entity name: '%1$s'.")
   SearchException invalidEntityName(String entityName);

   @Message(id = 14506, value = "Invalid type for '%1$s': the entity type must extend '%2$s'," +
         " but entity type '%3$s' does not.")
   SearchException invalidEntitySuperType(String entityName,
                                          @FormatWith(ClassFormatter.class) Class<?> expectedSuperType,
                                          @FormatWith(ClassFormatter.class) Class<?> actualJavaType);

   @LogMessage(level = WARN)
   @Message(id = 14507, value = "Data is '%s', while indexes are '%s', " +
         "in the meantime the index startup mode configuration is set to '%s': " +
         "this setting could lead to some inconsistency between the indexes and the data " +
         "in case of restarting the nodes.")
   void logIndexStartupModeMismatch(String data, String index, String startupMode);
}
