package org.infinispan.objectfilter.impl.hql;

import java.util.LinkedList;
import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class FilterEmbeddedEntityTypeDescriptor implements FilterTypeDescriptor {

   private final String entityType;
   private final List<String> propertyPath;
   private final ObjectPropertyHelper propertyHelper;

   /**
    * Creates a new {@link FilterEmbeddedEntityTypeDescriptor}.
    *
    * @param entityType     the entity into which this entity is embedded
    * @param path           the property path from the embedding entity to this entity
    * @param propertyHelper a helper for dealing with properties
    */
   public FilterEmbeddedEntityTypeDescriptor(String entityType, List<String> path, ObjectPropertyHelper propertyHelper) {
      this.entityType = entityType;
      this.propertyPath = path;
      this.propertyHelper = propertyHelper;
   }

   @Override
   public boolean hasProperty(String propertyName) {
      List<String> newPath = new LinkedList<String>(propertyPath);
      newPath.add(propertyName);
      return propertyHelper.hasProperty(entityType, newPath);
   }

   @Override
   public boolean hasEmbeddedProperty(String propertyName) {
      List<String> newPath = new LinkedList<String>(propertyPath);
      newPath.add(propertyName);
      return propertyHelper.hasEmbeddedProperty(entityType, newPath);
   }

   @Override
   public String getEntityType() {
      return entityType;
   }

   @Override
   public String toString() {
      return propertyPath.toString();
   }
}
