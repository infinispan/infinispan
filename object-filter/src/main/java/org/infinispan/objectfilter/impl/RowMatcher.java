package org.infinispan.objectfilter.impl;

import org.infinispan.objectfilter.impl.hql.RowPropertyHelper;
import org.infinispan.objectfilter.impl.predicateindex.RowMatcherEvalContext;

import java.util.Collections;
import java.util.List;

/**
 * A matcher for projection rows. This matcher is not stateless so it cannot be reused.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class RowMatcher extends BaseMatcher<RowPropertyHelper.RowMetadata, RowPropertyHelper.ColumnMetadata, Integer> {

   private final RowPropertyHelper.RowMetadata rowMetadata;

   public RowMatcher(RowPropertyHelper.ColumnMetadata[] columns) {
      super(new RowPropertyHelper(columns));
      rowMetadata = ((RowPropertyHelper) propertyHelper).getRowMetadata();
   }

   @Override
   protected RowMatcherEvalContext startMultiTypeContext(Object userContext, Object eventType, Object instance) {
      FilterRegistry<RowPropertyHelper.RowMetadata, RowPropertyHelper.ColumnMetadata, Integer> filterRegistry = getFilterRegistryForType(rowMetadata);
      if (filterRegistry != null) {
         RowMatcherEvalContext context = new RowMatcherEvalContext(userContext, eventType, instance, rowMetadata);
         context.initMultiFilterContext(filterRegistry);
         return context;
      }
      return null;
   }

   @Override
   protected RowMatcherEvalContext startSingleTypeContext(Object userContext, Object eventType, Object instance, MetadataAdapter<RowPropertyHelper.RowMetadata, RowPropertyHelper.ColumnMetadata, Integer> metadataAdapter) {
      if (Object[].class == instance.getClass()) {
         return new RowMatcherEvalContext(userContext, eventType, instance, rowMetadata);
      } else {
         return null;
      }
   }

   @Override
   protected FilterRegistry<RowPropertyHelper.RowMetadata, RowPropertyHelper.ColumnMetadata, Integer> getFilterRegistryForType(RowPropertyHelper.RowMetadata entityType) {
      return filtersByType.get(entityType);
   }

   @Override
   protected MetadataAdapter<RowPropertyHelper.RowMetadata, RowPropertyHelper.ColumnMetadata, Integer> createMetadataAdapter(RowPropertyHelper.RowMetadata rowMetadata) {
      return new MetadataAdapterImpl(rowMetadata);
   }

   private static class MetadataAdapterImpl implements MetadataAdapter<RowPropertyHelper.RowMetadata, RowPropertyHelper.ColumnMetadata, Integer> {

      private final RowPropertyHelper.RowMetadata rowMetadata;

      MetadataAdapterImpl(RowPropertyHelper.RowMetadata rowMetadata) {
         this.rowMetadata = rowMetadata;
      }

      @Override
      public String getTypeName() {
         return "Row";
      }

      @Override
      public RowPropertyHelper.RowMetadata getTypeMetadata() {
         return rowMetadata;
      }

      @Override
      public List<Integer> mapPropertyNamePathToFieldIdPath(String[] path) {
         if (path.length > 1) {
            throw new IllegalStateException("Nested attributes are not supported");
         }

         String columnName = path[0];
         for (RowPropertyHelper.ColumnMetadata c : rowMetadata.getColumns()) {
            if (c.getColumnName().equals(columnName)) {
               return Collections.singletonList(c.getColumnIndex());
            }
         }

         throw new IllegalArgumentException("Column not found : " + columnName);
      }

      @Override
      public RowPropertyHelper.ColumnMetadata makeChildAttributeMetadata(RowPropertyHelper.ColumnMetadata parentAttributeMetadata, Integer attribute) {
         if (parentAttributeMetadata != null) {
            throw new IllegalStateException("Nested attributes are not supported");
         }
         return rowMetadata.getColumns()[attribute];
      }

      @Override
      public boolean isComparableProperty(RowPropertyHelper.ColumnMetadata attributeMetadata) {
         Class<?> propertyType = attributeMetadata.getPropertyType();
         return propertyType.isPrimitive() || Comparable.class.isAssignableFrom(propertyType);
      }
   }
}
