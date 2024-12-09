package org.infinispan.search.mapper.log.impl;

import static org.jboss.logging.Logger.Level.WARN;

import org.hibernate.search.engine.environment.classpath.spi.ClassLoadingException;
import org.hibernate.search.util.common.SearchException;
import org.hibernate.search.util.common.logging.impl.ClassFormatter;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.FormatWith;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.ValidIdRange;

/**
 * Log abstraction for the search mapper module.
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

   @Message(id = 14505, value = "Unknown entity name: '%1$s'.")
   SearchException invalidEntityName(String entityName);

   @Message(id = 14506, value = "Invalid type for '%1$s': the entity type must extend '%2$s'," +
         " but entity type '%3$s' does not.")
   SearchException invalidEntitySuperType(String entityName,
         @FormatWith(ClassFormatter.class) Class<?> expectedSuperType,
         @FormatWith(ClassFormatter.class) Class<?> actualJavaType);

   @LogMessage(level = WARN)
   @Message(id = 14507, value = "Data is volatile, while indexes are stored on filesystem, " +
         "in the meantime the index startup mode configuration is set to none: " +
         "this setting could lead to some inconsistency between the indexes and the data " +
         "in case of restarting the nodes.")
   void volatileDataPersistentIndexesStartupModeNone();

   @LogMessage(level = WARN)
   @Message(id = 14508, value = "Data is persistent, while indexes are volatile, " +
         "in the meantime the index startup mode configuration is set to none or purge: " +
         "this setting could lead to some inconsistency between the indexes and the data " +
         "in case of restarting the nodes.")
   void persistentDataVolatileIndexesStartupModeNoneOrPurge();

}
