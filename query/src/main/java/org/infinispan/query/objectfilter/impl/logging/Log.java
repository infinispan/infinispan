package org.infinispan.query.objectfilter.impl.logging;

import java.util.List;

import org.infinispan.query.objectfilter.ParsingException;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.ValidIdRange;

/**
 * Log messages for the object filter parser backend. For this module, message ids ranging
 * from 28501 to 29000 inclusively have been reserved.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
@MessageLogger(projectCode = "ISPN")
@ValidIdRange(min = 28501, max = 29000)
public interface Log extends BasicLogger {

   @Message(id = 28501, value = "The type %s does not have an accessible property named '%s'.")
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
   IllegalStateException aggregationTypeNotSupportedException(String aggregationType);

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

   @Message(id = 28523, value = "Filters cannot use full-text or spatial queries")
   ParsingException filtersCannotUseFullTextOrSpatialSearchException();

   @Message(id = 28524, value = "Left side argument must be a property path")
   ParsingException getLeftSideMustBeAPropertyPath();

   @Message(id = 28525, value = "Invalid query: %s")
   ParsingException getQuerySyntaxException(String query, @Cause Throwable cause);

   @Message(id = 28526, value = "Invalid query: %s; Parser error messages: %s.")
   ParsingException getQuerySyntaxException(String query, List<?> parserErrorMessages);

   @Message(id = 28527, value = "Full-text queries cannot be applied to property '%2$s' in type %1$s unless the property is indexed.")
   ParsingException getFullTextQueryOnNotIndexedPropertyNotSupportedException(String typeName, String propertyName);

   @Message(id = 28528, value = "Error parsing content. Data not stored as protobuf?")
   ParsingException errorParsingProtobuf(@Cause Exception e);

   @Message(id = 28529, value = "Knn predicates are not allowed in the HAVING clause")
   ParsingException knnPredicateOnHavingClause();

   @Message(id = 28530, value = "Knn predicates cannot be applied to property '%2$s' in type %1$s unless the property is a vector field.")
   ParsingException knnPredicateOnNotVectorField(String typeName, String propertyName);

   @Message(id = 28531, value = "Knn vector parameter not valid.")
   ParsingException knnVectorParameterNotValid();

   @Message(id = 28532, value = "Knn predicates are not allowed in the FILTERING clause")
   ParsingException knnPredicateOnFilteringClause();

   @Message(id = 28533, value = "Function %s is not supported")
   ParsingException functionNotSupportedException(String functionName);

   @Message(id = 28534, value = "Spatial queries cannot be applied to property '%2$s' in type %1$s unless it is indexed as a spatial property.")
   ParsingException spatialQueryOnNotIndexedPropertyNotSupportedException(String typeName, String propertyName);

   @Message(id = 28535, value = "Query has too many nested parenthesized expressions: depth %d exceeds maximum allowed %d. " +
         "Use the system property '%s' to increase the limit.")
   ParsingException queryNestingDepthExceeded(int depth, int maxDepth, String propertyName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 28536, value = "Invalid query max nesting depth %d, using default %d.")
   void invalidNestingDepth(int configuredValue, int defaultValue);

   @Message(id = 28537, value = "Cannot assign a %s value to property '%s': incompatible with property type %s")
   ParsingException getIncompatibleUpdateValueException(String valueType, String propertyPath, Class<?> targetType);

   @Message(id = 28538, value = "Arithmetic operator %s requires numeric operands")
   ParsingException getArithmeticRequiresNumericOperandsException(String operator);

   @Message(id = 28539, value = "Value %s is not integral and cannot be assigned to integer property '%s'")
   ParsingException getNonIntegralValueException(Number value, String propertyPath);

   @Message(id = 28540, value = "Division or modulo by zero in constant expression")
   ParsingException getConstantDivisionByZeroException();

   @Message(id = 28541, value = "Value %s is out of range for property '%s' of type %s")
   ParsingException getValueOutOfRangeException(Number value, String propertyPath, Class<?> targetType);

   @Message(id = 28542, value = "Unknown function '%s' in Ickle statement")
   ParsingException getUnknownUpdateFunctionException(String functionName);

   @Message(id = 28543, value = "Division by zero")
   ArithmeticException divisionByZeroException();

   @Message(id = 28544, value = "Missing value for parameter '%s'")
   IllegalArgumentException missingParameterException(String name);

   @Message(id = 28545, value = "Cannot apply operator '%s' to operands of type %s and %s")
   IllegalArgumentException cannotApplyOperatorException(String operator, String leftType, String rightType);

   @Message(id = 28546, value = "Cannot negate a value of type %s")
   IllegalArgumentException cannotNegateException(String type);

   @Message(id = 28547, value = "Unknown function '%s'")
   IllegalArgumentException unknownFunctionException(String name);

   @Message(id = 28548, value = "Function %s expects %d argument(s) but got %d")
   IllegalArgumentException functionArityException(String name, int expected, int actual);

   @Message(id = 28549, value = "Function %s expects between %d and %d argument(s) but got %d")
   IllegalArgumentException functionArityRangeException(String name, int min, int max, int actual);

   @Message(id = 28550, value = "Function %s expects a numeric argument but got %s")
   IllegalArgumentException functionNumericArgException(String name, String type);

   @Message(id = 28551, value = "Function %s argument %d must be a number, got '%s'")
   IllegalArgumentException functionArgumentNotNumberException(String name, int argIndex, String value);

   @Message(id = 28552, value = "Function %s expects numeric arguments")
   IllegalArgumentException functionNumericArgsException(String name);

   @Message(id = 28553, value = "Function %s cannot parse '%s' as a number")
   IllegalArgumentException functionCannotParseNumberException(String name, String value);

   @Message(id = 28554, value = "Function %s cannot convert a value of type %s")
   IllegalArgumentException functionCannotConvertTypeException(String name, String type);

   @Message(id = 28555, value = "substring: invalid range [%d, %d) for string of length %d")
   IllegalArgumentException substringRangeException(int begin, int end, int length);

   @Message(id = 28556, value = "dateAdd: unknown unit '%s', expected one of %s")
   IllegalArgumentException dateAddUnknownUnitException(String unit, Object expected);

   @Message(id = 28557, value = "Function %s expects a date/time argument (java.util.Date, java.util.Calendar or a java.time type) but got %s")
   IllegalArgumentException functionDateArgException(String name, String type);

   @Message(id = 28558, value = "Function already registered: %s")
   IllegalArgumentException functionAlreadyRegisteredException(String name);
}
