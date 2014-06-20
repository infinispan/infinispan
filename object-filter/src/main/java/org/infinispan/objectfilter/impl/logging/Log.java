package org.infinispan.objectfilter.impl.logging;

import org.hibernate.hql.ParsingException;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Log messages for the object filter parser backend..
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {

   @Message(id = 402, value = "The type %s has no property named '%s'.")
   ParsingException getNoSuchPropertyException(String typeName, String propertyName);

   @Message(id = 404, value = "Unknown alias: %s.")
   ParsingException getUnknownAliasException(String unknownAlias);

   @Message(id = 405, value = "Property %2$s can not be selected from type %1$s since it is an embedded entity.")
   ParsingException getProjectionOfCompleteEmbeddedEntitiesNotSupportedException(String typeName, String propertyPath);

   @Message(id = 406, value = "The property %s is an embedded entity and does not allow comparison predicates")
   ParsingException getPredicatesOnCompleteEmbeddedEntitiesNotAllowedException(String propertyPath);

   @Message(id = 407, value = "Invalid numeric literal '%s'")
   ParsingException getInvalidNumericLiteralException(String value);

   @Message(id = 408, value = "Invalid date literal '%s'")
   ParsingException getInvalidDateLiteralException(String value);

   @Message(id = 409, value = "Invalid boolean literal '%s'")
   ParsingException getInvalidBooleanLiteralException(String value);

   @Message(id = 410, value = "Invalid enum literal '%s' for enum type %s")
   ParsingException getInvalidEnumLiteralException(String value, String enumType);
}
