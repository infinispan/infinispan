package org.infinispan.objectfilter.impl.syntax.parser;

import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
final class EmbeddedEntityTypeDescriptor<TypeMetadata> extends EntityTypeDescriptor<TypeMetadata> {

   private final List<String> propertyPath;

   /**
    * Creates a new {@link EmbeddedEntityTypeDescriptor}.
    *
    * @param entityType     the entity into which this entity is embedded
    * @param entityMetadata the actual entity type representation
    * @param path           the property path from the embedding entity to this entity
    */
   EmbeddedEntityTypeDescriptor(String entityType, TypeMetadata entityMetadata, List<String> path) {
      super(entityType, entityMetadata);
      this.propertyPath = path;
   }

   @Override
   public String[] makePath(String propName) {
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
