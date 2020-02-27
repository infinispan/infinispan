package org.infinispan.search.mapper.common;

import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;
import org.infinispan.search.mapper.common.impl.EntityReferenceImpl;
import org.infinispan.search.mapper.mapping.SearchMappingBuilder;

/**
 * A reference to an indexed entity.
 */
public interface EntityReference {

   static EntityReference withDefaultName(Class<?> type, Object id) {
      return new EntityReferenceImpl(PojoRawTypeIdentifier.of(type), type.getSimpleName(), id);
   }

   /**
    * @return The type of the referenced entity.
    */
   Class<?> getType();

   /**
    * @return The name of the referenced entity in the Hibernate Search mapping.
    * @see SearchMappingBuilder#addEntityType(Class, String)
    */
   String getName();

   /**
    * @return The identifier of the referenced entity, i.e. the value of the property marked as {@code @DocumentId}.
    */
   Object getId();

}
