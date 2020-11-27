package org.infinispan.search.mapper.common;

import org.infinispan.search.mapper.mapping.SearchMappingBuilder;

/**
 * A reference to an indexed entity.
 */
public interface EntityReference {

   /**
    * @return The type of the referenced entity.
    */
   Class<?> type();

   /**
    * @return The name of the referenced entity in the Hibernate Search mapping.
    * @see SearchMappingBuilder#addEntityType(Class, String)
    */
   String name();

   /**
    * @return The key of the referenced entity.
    */
   Object key();

}
