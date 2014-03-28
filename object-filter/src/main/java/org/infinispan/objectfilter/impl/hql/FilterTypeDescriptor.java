package org.infinispan.objectfilter.impl.hql;

import org.hibernate.hql.ast.TypeDescriptor;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public interface FilterTypeDescriptor extends TypeDescriptor {

   /**
    * Returns the Java type of the represented entity.
    *
    * @return the Java type of the represented entity
    */
   String getEntityType();

   /**
    * Whether the given property denotes an embedded entity or not.
    *
    * @param propertyName the name of the property
    * @return {@code true} if the given property denotes an entity embedded into this one, {@code false} otherwise.
    */
   boolean hasEmbeddedProperty(String propertyName);
}
