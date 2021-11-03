package org.infinispan.query.core.impl;

import org.infinispan.commons.CacheException;
import org.infinispan.objectfilter.ParsingException;
import org.infinispan.partitionhandling.AvailabilityException;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.ValidIdRange;

//TODO [anistor] query-core and query modules share the id range now and in the future these loggers will probably merge
/**
 * Log abstraction for the query-core module. For this module, message ids ranging from 14001 to 14500 inclusively have been
 * reserved.
 *
 * @author anistor@redhat.com
 * @since 10.1
 */
@MessageLogger(projectCode = "ISPN")
@ValidIdRange(min = 14001, max = 14500)
public interface Log extends BasicLogger {

   String LOG_ROOT = "org.infinispan.";
   Log CONTAINER = Logger.getMessageLogger(Log.class, LOG_ROOT + "CONTAINER");

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

   @Message(value = "Cannot execute query: cluster is operating in degraded mode and partition handling configuration doesn't allow reads and writes.", id = 14042)
   AvailabilityException partitionDegraded();

   @Message(value = "Only DELETE statements are supported by executeStatement", id = 14056)
   CacheException unsupportedStatement();

   @Message(value = "DELETE statements cannot use paging (firstResult/maxResults)", id = 14057)
   CacheException deleteStatementsCannotUsePaging();

   @Message(value = "Projections are not supported with entryIterator()", id = 14058)
   CacheException entryIteratorDoesNotAllowProjections();
}
