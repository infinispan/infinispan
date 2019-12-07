package org.infinispan.query.core.impl;

import org.infinispan.commons.CacheException;
import org.infinispan.objectfilter.ParsingException;
import org.infinispan.partitionhandling.AvailabilityException;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Log abstraction for the query module. For this module, message ids ranging from 14001 to 14800 inclusively have been
 * reserved.
 *
 * @author anistor@redhat.com
 * @since 10.1
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {

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

   @Message(value = "infinispan-query-core.jar module is in the classpath but has not been properly initialised!", id = 14038)
   CacheException queryModuleNotInitialised();

   @Message(value = "Cannot execute query: cluster is operating in degraded mode and partition handling configuration doesn't allow reads and writes.", id = 14042)
   AvailabilityException partitionDegraded();
}
