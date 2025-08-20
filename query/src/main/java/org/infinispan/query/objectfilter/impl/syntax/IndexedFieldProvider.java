package org.infinispan.query.objectfilter.impl.syntax;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
@FunctionalInterface
public interface IndexedFieldProvider<TypeMetadata> {

   FieldIndexingMetadata<TypeMetadata> get(TypeMetadata typeMetadata);

   interface FieldIndexingMetadata<TypeMetadata> {

      boolean hasProperty(String[] propertyPath);

      /**
       * Checks if the property of the indexed entity is indexed.
       *
       * @param propertyPath the path of the property
       * @return {@code true} if the property is indexed, {@code false} otherwise.
       */
      boolean isSearchable(String[] propertyPath);

      /**
       * Checks if the property of the indexed entity is analyzed.
       *
       * @param propertyPath the path of the property
       * @return {@code true} if the property is analyzed, {@code false} otherwise.
       */
      boolean isAnalyzed(String[] propertyPath);

      /**
       * Checks if the property of the indexed entity is normalized.
       *
       * @param propertyPath the path of the property
       * @return {@code true} if the property is normalized, {@code false} otherwise.
       */
      boolean isNormalized(String[] propertyPath);

      /**
       * Checks if the property of the indexed entity is projectable.
       *
       * @param propertyPath the path of the property
       * @return {@code true} if the property is projectable, {@code false} otherwise.
       */
      boolean isProjectable(String[] propertyPath);

      /**
       * Checks if the property of the indexed entity is aggregable.
       *
       * @param propertyPath the path of the property
       * @return {@code true} if the property is aggregable, {@code false} otherwise.
       */
      boolean isAggregable(String[] propertyPath);

      /**
       * Checks if the property of the indexed entity is sortable.
       *
       * @param propertyPath the path of the property
       * @return {@code true} if the property is sortable, {@code false} otherwise.
       */
      boolean isSortable(String[] propertyPath);

      boolean isVector(String[] propertyPath);

      /**
       * Checks if the property of the indexed entity is a spatial property.
       *
       * @param propertyPath the path of the property
       * @return {@code true} if the property is spatial, {@code false} otherwise.
       */
      boolean isSpatial(String[] propertyPath);

      Object getNullMarker(String[] propertyPath);

      TypeMetadata keyType(String property);

   }

   static <TM> FieldIndexingMetadata<TM> noIndexing() {
      return new FieldIndexingMetadata<>() {
         @Override
         public boolean hasProperty(String[] propertyPath) {
            return false;
         }
         @Override
         public boolean isSearchable(String[] propertyPath) {
            return false;
         }
         @Override
         public boolean isAnalyzed(String[] propertyPath) {
            return false;
         }
         @Override
         public boolean isNormalized(String[] propertyPath) {
            return false;
         }
         @Override
         public boolean isProjectable(String[] propertyPath) {
            return false;
         }
         @Override
         public boolean isAggregable(String[] propertyPath) {
            return false;
         }
         @Override
         public boolean isSortable(String[] propertyPath) {
            return false;
         }
         @Override
         public boolean isVector(String[] propertyPath) {
            return false;
         }
         @Override
         public boolean isSpatial(String[] propertyPath) {
            return false;
         }
         @Override
         public Object getNullMarker(String[] propertyPath) {
            return null;
         }
         @Override
         public TM keyType(String property) {
            return null;
         }
      };
   }
}
