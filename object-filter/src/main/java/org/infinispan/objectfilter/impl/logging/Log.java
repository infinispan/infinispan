package org.infinispan.objectfilter.impl.logging;

import java.util.List;

import org.antlr.runtime.RecognitionException;
import org.infinispan.objectfilter.ParsingException;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Log messages for the object filter parser backend. For this module, message ids ranging
 * from 28501 to 29000 inclusively have been reserved.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {

   @Message(id = 28501, value = "The type %s has no property named '%s'.")
   ParsingException getNoSuchPropertyException(String typeName, String propertyName);

   @Message(id = 28502, value = "Unknown alias: %s.")
   ParsingException getUnknownAliasException(String unknownAlias);

   @Message(id = 28503, value = "Property %2$s can not be selected from type %1$s since it is an embedded entity.")
   ParsingException getProjectionOfCompleteEmbeddedEntitiesNotSupportedException(String typeName, String propertyPath);

   @Message(id = 28504, value = "The property %s is an embedded entity and does not allow comparison predicates")
   ParsingException getPredicatesOnCompleteEmbeddedEntitiesNotAllowedException(String propertyPath);

   @Message(id = 28505, value = "Invalid numeric literal '%s'")
   ParsingException getInvalidNumericLiteralException(String value);

   @Message(id = 28506, value = "Invalid date literal '%s'")
   ParsingException getInvalidDateLiteralException(String value);

   @Message(id = 28507, value = "Invalid boolean literal '%s'")
   ParsingException getInvalidBooleanLiteralException(String value);

   @Message(id = 28508, value = "Invalid enum literal '%s' for enum type %s")
   ParsingException getInvalidEnumLiteralException(String value, String enumType);

   @Message(id = 28509, value = "Filters cannot use grouping or aggregations")
   ParsingException getFiltersCannotUseGroupingOrAggregationException();

   @Message(id = 28510, value = "Unknown entity name %s")
   IllegalStateException getUnknownEntity(String entityType);

   @Message(id = 28511, value = "namedParameters cannot be null")
   IllegalArgumentException getNamedParametersCannotBeNull();

   @Message(id = 28512, value = "Aggregation %s is not supported")
   IllegalStateException getAggregationTypeNotSupportedException(String aggregationType);

   @Message(id = 28513, value = "Aggregation AVG cannot be applied to property of type %s")
   IllegalStateException getAVGCannotBeAppliedToPropertyOfType(String typeName);

   @Message(id = 28514, value = "%s aggregation can only be applied to property references.")
   ParsingException getAggregationCanOnlyBeAppliedToPropertyReferencesException(String aggregationType);

   @Message(id = 28515, value = "Cannot have aggregate functions in the WHERE clause : %s.")
   ParsingException getNoAggregationsInWhereClauseException(String aggregationType);

   @Message(id = 28516, value = "Cannot have aggregate functions in the GROUP BY clause : %s.")
   ParsingException getNoAggregationsInGroupByClauseException(String aggregationType);

   @Message(id = 28517, value = "The predicate %s can not be added since there may be only one root predicate.")
   IllegalStateException getNotMoreThanOnePredicateInRootOfWhereClauseAllowedException(Object predicate);

   @Message(id = 28518, value = "The predicate %s can not be added since there may be only one sub-predicate in a NOT predicate.")
   IllegalStateException getNotMoreThanOnePredicateInNegationAllowedException(Object predicate);

   @Message(id = 28519, value = "Cannot apply predicates directly to an entity alias: %s")
   ParsingException getPredicatesOnEntityAliasNotAllowedException(String alias);

   @Message(id = 28520, value = "Full-text queries are not allowed in the HAVING clause")
   ParsingException getFullTextQueriesNotAllowedInHavingClauseException();

   @Message(id = 28521, value = "Full-text queries cannot be applied to property '%2$s' in type %1$s unless the property is indexed and analyzed.")
   ParsingException getFullTextQueryOnNotAalyzedPropertyNotSupportedException(String typeName, String propertyName);

   @Message(id = 28522, value = "No relational queries can be applied to property '%2$s' in type %1$s since the property is analyzed.")
   ParsingException getQueryOnAnalyzedPropertyNotSupportedException(String typeName, String propertyName);

   @Message(id = 28523, value = "Filters cannot use full-text searches")
   ParsingException getFiltersCannotUseFullTextSearchException();

   @Message(id = 28524, value = "Left side argument must be a property path")
   ParsingException getLeftSideMustBeAPropertyPath();

   @Message(id = 28525, value = "Invalid query: %s")
   ParsingException getQuerySyntaxException(String query, @Cause RecognitionException cause);

   @Message(id = 28526, value = "Invalid query: %s; Parser error messages: %s.")
   ParsingException getQuerySyntaxException(String query, List<?> parserErrorMessages);

   @Message(id = 28527, value = "Full-text queries cannot be applied to property '%2$s' in type %1$s unless the property is indexed.")
   ParsingException getFullTextQueryOnNotIndexedPropertyNotSupportedException(String typeName, String propertyName);

   @Message(id = 28528, value = "Error parsing content. Data not stored as protobuf?")
   ParsingException errorParsingProtobuf(@Cause Exception e);

}
