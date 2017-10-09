package org.infinispan.objectfilter.impl.syntax;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
@FunctionalInterface
public interface IndexedFieldProvider<TypeMetadata> {

   FieldIndexingMetadata get(TypeMetadata typeMetadata);

   interface FieldIndexingMetadata {

      /**
       * Checks if the property of the indexed entity is indexed.
       *
       * @param propertyPath the path of the property
       * @return {@code true} if the property is indexed, {@code false} otherwise.
       */
      boolean isIndexed(String[] propertyPath);

      /**
       * Checks if the property of the indexed entity is analyzed.
       *
       * @param propertyPath the path of the property
       * @return {@code true} if the property is analyzed, {@code false} otherwise.
       */
      boolean isAnalyzed(String[] propertyPath);

      /**
       * Checks if the property of the indexed entity is stored.
       *
       * @param propertyPath the path of the property
       * @return {@code true} if the property is stored, {@code false} otherwise.
       */
      boolean isStored(String[] propertyPath);

      Object getNullMarker(String[] propertyPath);
   }

   FieldIndexingMetadata NO_INDEXING = new FieldIndexingMetadata() {
      @Override
      public boolean isIndexed(String[] propertyPath) {
         return false;
      }

      @Override
      public boolean isAnalyzed(String[] propertyPath) {
         return false;
      }

      @Override
      public boolean isStored(String[] propertyPath) {
         return false;
      }

      @Override
      public Object getNullMarker(String[] propertyPath) {
         return null;
      }
   };
}
