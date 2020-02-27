package org.infinispan.search.mapper.log.impl;

import org.hibernate.search.engine.environment.classpath.spi.ClassLoadingException;
import org.hibernate.search.mapper.pojo.logging.spi.PojoTypeModelFormatter;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeModel;
import org.hibernate.search.util.common.SearchException;
import org.hibernate.search.util.common.logging.impl.ClassFormatter;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.FormatWith;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.ValidIdRange;

@MessageLogger(projectCode = "ISPN")
@ValidIdRange(min = 750000, max = 759999)
// TODO verify if the range id [750000 - 759999] is good for this project
public interface Log extends BasicLogger {

   int ID_OFFSET_1 = 750000;

   @Message(id = ID_OFFSET_1 + 1, value = "Unable to find property '%2$s' on type '%1$s'.")
   SearchException cannotFindProperty(@FormatWith(PojoTypeModelFormatter.class) PojoRawTypeModel<?> typeModel,
                                      String propertyName);

   @Message(id = ID_OFFSET_1 + 2, value = "Exception while retrieving the type model for '%1$s'.")
   SearchException errorRetrievingTypeModel(@FormatWith(ClassFormatter.class) Class<?> clazz, @Cause Exception cause);

   @Message(id = ID_OFFSET_1 + 3, value = "Exception while retrieving property type model for '%1$s' on '%2$s'")
   SearchException errorRetrievingPropertyTypeModel(String propertyModelName, @FormatWith(PojoTypeModelFormatter.class)
         PojoRawTypeModel<?> parentTypeModel, @Cause Exception cause);

   @Message(id = ID_OFFSET_1 + 4, value = "Multiple entity types configured with the same name '%1$s': '%2$s', '%3$s'")
   SearchException multipleEntityTypesWithSameName(String entityName, Class<?> previousType, Class<?> type);

   @Message(id = ID_OFFSET_1 + 5, value = "Infinispan Search Mapper does not support named types." +
         " The type with name '%1$s' does not exist.")
   SearchException namedTypesNotSupported(String name);

   @Message(id = ID_OFFSET_1 + 6, value = "Unable to load class [%1$s]")
   ClassLoadingException unableToLoadTheClass(String className, @Cause Throwable cause);

}
