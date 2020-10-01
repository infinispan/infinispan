package org.infinispan.search.mapper.log.impl;

import org.hibernate.search.engine.environment.classpath.spi.ClassLoadingException;
import org.hibernate.search.util.common.SearchException;
import org.hibernate.search.util.common.logging.impl.ClassFormatter;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.FormatWith;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.ValidIdRange;

/**
 * Log abstraction for the search mapper module. For this module, message ids ranging from 14501 to 14800 inclusively
 * have been reserved.
 *
 * @since 12.0
 */
@MessageLogger(projectCode = "ISPN")
@ValidIdRange(min = 14501, max = 14800)
public interface Log extends BasicLogger {

   @Message(id = 14501, value = "Exception while retrieving the type model for '%1$s'.")
   SearchException errorRetrievingTypeModel(@FormatWith(ClassFormatter.class) Class<?> clazz, @Cause Exception cause);

   @Message(id = 14502, value = "Multiple entity types configured with the same name '%1$s': '%2$s', '%3$s'")
   SearchException multipleEntityTypesWithSameName(String entityName, Class<?> previousType, Class<?> type);

   @Message(id = 14503, value = "Infinispan Search Mapper does not support named types. The type with name '%1$s' does not exist.")
   SearchException namedTypesNotSupported(String name);

   @Message(id = 14504, value = "Unable to load class [%1$s]")
   ClassLoadingException unableToLoadTheClass(String className, @Cause Throwable cause);

}
