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
}
