package org.infinispan.objectfilter.impl.logging;

import org.hibernate.hql.ParsingException;
import org.jboss.logging.BasicLogger;
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
}
