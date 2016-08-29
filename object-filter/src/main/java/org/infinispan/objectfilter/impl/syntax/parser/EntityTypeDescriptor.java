package org.infinispan.objectfilter.impl.syntax.parser;

import org.infinispan.objectfilter.impl.util.StringHelper;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
class EntityTypeDescriptor<TypeMetadata> implements TypeDescriptor<TypeMetadata> {

   private final String typeName;

   private final TypeMetadata entityMetadata;

   EntityTypeDescriptor(String typeName, TypeMetadata entityMetadata) {
      this.typeName = typeName;
      this.entityMetadata = entityMetadata;
   }

   @Override
   public String getTypeName() {
      return typeName;
   }

   @Override
   public TypeMetadata getTypeMetadata() {
      return entityMetadata;
   }

   public String[] makePath(String propName) {
      return StringHelper.split(propName);
   }

   @Override
   public String toString() {
      return typeName;
   }
}
