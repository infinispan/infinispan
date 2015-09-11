package org.infinispan.objectfilter.impl;

import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.infinispan.objectfilter.impl.hql.FilterProcessingChain;
import org.infinispan.objectfilter.impl.hql.RowPropertyHelper;
import org.infinispan.objectfilter.impl.predicateindex.RowMatcherEvalContext;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class RowMatcher extends BaseMatcher<RowPropertyHelper.RowMetadata, RowPropertyHelper.ColumnMetadata, Integer> {

   private final RowPropertyHelper.RowMetadata rowMetadata;

   private final RowPropertyHelper propertyHelper;

   private final EntityNamesResolver entityNamesResolver = new EntityNamesResolver() {

      @Override
      public Class<?> getClassFromName(String entityName) {
         return Object[].class;
      }
   };

   public RowMatcher(RowPropertyHelper.ColumnMetadata[] columns) {
      rowMetadata = new RowPropertyHelper.RowMetadata(columns);
      propertyHelper = new RowPropertyHelper(rowMetadata);
   }

   @Override
   protected RowMatcherEvalContext startMultiTypeContext(Object userContext, Object instance, Object eventType) {
      FilterRegistry<RowPropertyHelper.RowMetadata, RowPropertyHelper.ColumnMetadata, Integer> filterRegistry = getFilterRegistryForType(rowMetadata);
      if (filterRegistry != null) {
         RowMatcherEvalContext context = new RowMatcherEvalContext(userContext, instance, rowMetadata, eventType);
         context.initMultiFilterContext(filterRegistry);
         return context;
      }
      return null;
   }

   @Override
   protected RowMatcherEvalContext startSingleTypeContext(Object userContext, Object instance, MetadataAdapter<RowPropertyHelper.RowMetadata, RowPropertyHelper.ColumnMetadata, Integer> metadataAdapter, Object eventType) {
      if (Object[].class == instance.getClass()) {
         return new RowMatcherEvalContext(userContext, instance, rowMetadata, eventType);
      } else {
         return null;
      }
   }

   @Override
   protected FilterProcessingChain<RowPropertyHelper.RowMetadata> createFilterProcessingChain(Map<String, Object> namedParameters) {
      return FilterProcessingChain.build(entityNamesResolver, propertyHelper, namedParameters);
   }

   @Override
   protected FilterRegistry<RowPropertyHelper.RowMetadata, RowPropertyHelper.ColumnMetadata, Integer> getFilterRegistryForType(RowPropertyHelper.RowMetadata entityType) {
      return filtersByType.get(entityType);
   }

   @Override
   public RowPropertyHelper getPropertyHelper() {
      return propertyHelper;
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
      public List<Integer> translatePropertyPath(List<String> path) {
         if (path.size() > 1) {
            throw new IllegalStateException("Nested attributes are not supported");
         }

         String columnName = path.get(0);
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
