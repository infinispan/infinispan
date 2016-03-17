package org.infinispan.query.logging;

import java.io.IOException;
import java.util.List;

import org.hibernate.hql.ParsingException;
import org.hibernate.search.backend.LuceneWork;
import org.infinispan.commons.CacheException;
import org.infinispan.remoting.transport.Address;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

import static org.jboss.logging.Logger.Level.DEBUG;
import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.INFO;
import static org.jboss.logging.Logger.Level.TRACE;
import static org.jboss.logging.Logger.Level.WARN;

/**
 * Log abstraction for the query module. For this module, message ids
 * ranging from 14001 to 14800 inclusively have been reserved.
 *
 * @author Galder Zamarreño
 * @author Sanne Grinovero
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.util.logging.Log {

   @LogMessage(level = ERROR)
   @Message(value = "Could not locate key class %s", id = 14001)
   void keyClassNotFound(String keyClassName, @Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Cannot instantiate Transformer class %s", id = 14002)
   void couldNotInstantiaterTransformerClass(Class<?> transformer, @Cause Exception e);

   @LogMessage(level = INFO)
   @Message(value = "Registering Query interceptor for cache %s", id = 14003)
   void registeringQueryInterceptor(String cacheName);

   @LogMessage(level = DEBUG)
   @Message(value = "Custom commands backend initialized backing index %s", id = 14004)
   void commandsBackendInitialized(String indexName);

   @LogMessage(level = TRACE)
   @Message(value = "Sent list of LuceneWork %s to node %s", id = 14005)
   void workListRemotedTo(Object workList, Address primaryNodeAddress);

   @LogMessage(level = TRACE)
   @Message(value = "Apply list of LuceneWork %s delegating to local indexing engine", id = 14006)
   void applyingChangeListLocally(List<LuceneWork> workList);

   @LogMessage(level = DEBUG)
   @Message(value = "Going to ship list of LuceneWork %s to a remote master indexer", id = 14007)
   void applyingChangeListRemotely(List<LuceneWork> workList);

   @LogMessage(level = WARN)
   @Message(value = "Index named '%1$s' is ignoring configuration option 'directory_provider' set '%2$s':" +
         " overridden to use the Infinispan Directory", id = 14008)
   void ignoreDirectoryProviderProperty(String indexName, String directoryOption);

   @LogMessage(level = WARN)
   @Message(value = "Indexed type '%1$s' is using a default Transformer. This is slow! Register a custom implementation using @Transformable", id = 14009)
   void typeIsUsingDefaultTransformer(Class<?> keyClass);

   @Message(value = "An IOException happened where none where expected", id = 14010)
   CacheException unexpectedIOException(@Cause IOException e);

   @LogMessage(level = WARN)
   @Message(value = "Some indexing work was lost because of an InterruptedException", id = 14011)
   void interruptedWhileBufferingWork(@Cause InterruptedException e);

   @LogMessage(level = DEBUG)
   @Message(value = "Waiting for index lock was successful: '%1$s'", id = 14012)
   void waitingForLockAcquired(boolean waitForAvailabilityInternal);

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

   @Message(value = "Error executing MassIndexer", id = 14018)
   CacheException errorExecutingMassIndexer(@Cause Throwable cause);

   @Message(value = "Cannot run Lucene queries on a cache that does not have indexing enabled", id = 14019)
   IllegalStateException cannotRunLuceneQueriesIfNotIndexed();

   @Message(value = "Query parameter '%s' was not set", id = 14020)
   IllegalStateException queryParameterNotSet(String paramName);

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

   @LogMessage(level = WARN)
   @Message(value = "Detected an undeclared indexed entity class in cache %s: %s. Autodetection support will be removed in Infinispan 9.0.", id = 14028)
   void detectedUnknownIndexedEntity(String cacheName, String className);

   //todo [anistor] This should become a CacheException in Infinispan 9.0
   @LogMessage(level = WARN)
   @Message(value = "Found undeclared indexable types in cache %s : %s. No indexes were created for these types because autodetection is not enabled for this cache.", id = 14029)
   void detectedUnknownIndexedEntities(String cacheName, String classNames);
}
