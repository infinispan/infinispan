package org.infinispan.objectfilter.impl.hql;

import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
final class FilterEmbeddedEntityTypeDescriptor implements FilterTypeDescriptor {

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
   FilterEmbeddedEntityTypeDescriptor(String entityType, List<String> path, ObjectPropertyHelper propertyHelper) {
      this.entityType = entityType;
      this.propertyPath = path;
      this.propertyHelper = propertyHelper;
   }

   @Override
   public boolean hasProperty(String propertyName) {
      return propertyHelper.hasProperty(entityType, makeJoinedPath(propertyName));
   }

   @Override
   public boolean hasEmbeddedProperty(String propertyName) {
      return propertyHelper.hasEmbeddedProperty(entityType, makeJoinedPath(propertyName));
   }

   @Override
   public String[] makeJoinedPath(String propName) {
      String[] newPath = new String[propertyPath.size() + 1];
      newPath = propertyPath.toArray(newPath);
      newPath[newPath.length - 1] = propName;
      return newPath;
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
