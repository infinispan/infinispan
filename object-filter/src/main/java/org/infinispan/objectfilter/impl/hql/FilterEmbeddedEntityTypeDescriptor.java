package org.infinispan.objectfilter.impl.hql;

import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
final class FilterEmbeddedEntityTypeDescriptor extends FilterEntityTypeDescriptor {

   private final List<String> propertyPath;

   /**
    * Creates a new {@link FilterEmbeddedEntityTypeDescriptor}.
    *
    * @param entityType     the entity into which this entity is embedded
    * @param propertyHelper a helper for dealing with properties
    * @param path           the property path from the embedding entity to this entity
    */
   FilterEmbeddedEntityTypeDescriptor(String entityType, ObjectPropertyHelper propertyHelper, List<String> path) {
      super(entityType, propertyHelper);
      this.propertyPath = path;
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
   public String toString() {
      return propertyPath.toString();
   }
}
