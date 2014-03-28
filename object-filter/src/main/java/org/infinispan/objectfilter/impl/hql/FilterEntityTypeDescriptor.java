package org.infinispan.objectfilter.impl.hql;

import org.infinispan.objectfilter.impl.util.StringHelper;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class FilterEntityTypeDescriptor implements FilterTypeDescriptor {

   private final String entityType;
   private final ObjectPropertyHelper propertyHelper;

   public FilterEntityTypeDescriptor(String entityType, ObjectPropertyHelper propertyHelper) {
      this.entityType = entityType;
      this.propertyHelper = propertyHelper;
   }

   @Override
   public boolean hasProperty(String propertyName) {
      return propertyHelper.hasProperty(entityType, StringHelper.splitPropertyPath(propertyName));
   }

   @Override
   public String getEntityType() {
      return entityType;
   }

   @Override
   public boolean hasEmbeddedProperty(String propertyName) {
      return propertyHelper.hasEmbeddedProperty(entityType, StringHelper.splitPropertyPath(propertyName));
   }

   @Override
   public String toString() {
      return entityType;
   }
}
