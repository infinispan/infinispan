package org.infinispan.query.mapper.mapping;

import java.util.Set;

import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;

/**
 * Can convert an entity before indexing it.
 *
 * @author Fabio Massimo Ercoli
 */
public interface EntityConverter {

   /**
    * @return The type that is supposed to be converted
    */
   Class<?> targetType();

   /**
    * @return The resulting converted type
    */
   Set<PojoRawTypeIdentifier<?>> convertedTypeIdentifiers();

   /**
    * @param type The type to check
    * @return Whether the type is supposed to be converted
    */
   boolean typeIsIndexed(Class<?> type);

   /**
    * Perform the conversion.
    *
    * @param entity The entity to convert
    * @param providedId The id that could be used in the conversion process
    * @return The converted entity
    */
   ConvertedEntity convert(Object entity, Object providedId);

   /**
    * The result of an entity conversion
    */
   interface ConvertedEntity {

      boolean skip();

      String entityName();

      Object value();
   }

}
