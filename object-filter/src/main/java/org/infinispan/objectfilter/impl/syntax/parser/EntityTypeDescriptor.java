package org.infinispan.objectfilter.impl.syntax.parser;

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
      return new String[]{propName};
   }

   @Override
   public String toString() {
      return "EntityTypeDescriptor{typeName='" + typeName + "'}";
   }
}
