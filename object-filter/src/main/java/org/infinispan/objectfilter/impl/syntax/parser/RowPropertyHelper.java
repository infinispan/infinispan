package org.infinispan.objectfilter.impl.syntax.parser;

import java.util.Collections;
import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class RowPropertyHelper extends ObjectPropertyHelper<RowPropertyHelper.RowMetadata> {

   public static final class RowMetadata {

      private final ColumnMetadata[] columns;

      RowMetadata(ColumnMetadata[] columns) {
         this.columns = columns;
      }

      public ColumnMetadata[] getColumns() {
         return columns;
      }
   }

   public static final class ColumnMetadata {

      private final int columnIndex;

      private final String columnName;

      private final Class<?> type;

      public ColumnMetadata(int columnIndex, String columnName, Class<?> type) {
         this.columnIndex = columnIndex;
         this.columnName = columnName;
         this.type = type;
      }

      public Object getValue(Object instance) {
         return ((Object[]) instance)[columnIndex];
      }

      public int getColumnIndex() {
         return columnIndex;
      }

      public String getColumnName() {
         return columnName;
      }

      public Class<?> getPropertyType() {
         return type;
      }

      @Override
      public String toString() {
         return "ColumnMetadata{" +
               "columnIndex=" + columnIndex +
               ", columnName='" + columnName + '\'' +
               ", type=" + type +
               '}';
      }
   }

   private final RowMetadata rowMetadata;

   public RowPropertyHelper(RowPropertyHelper.ColumnMetadata[] columns) {
      this.rowMetadata = new RowPropertyHelper.RowMetadata(columns);
   }

   public RowMetadata getRowMetadata() {
      return rowMetadata;
   }

   @Override
   public RowMetadata getEntityMetadata(String typeName) {
      // the type name is ignored in this case!
      return rowMetadata;
   }

   @Override
   public List<?> mapPropertyNamePathToFieldIdPath(RowMetadata type, String[] propertyPath) {
      if (propertyPath.length > 1) {
         throw new IllegalStateException("Nested attributes are not supported");
      }

      String columnName = propertyPath[0];
      for (RowPropertyHelper.ColumnMetadata c : rowMetadata.getColumns()) {
         if (c.getColumnName().equals(columnName)) {
            return Collections.singletonList(c.getColumnIndex());
         }
      }

      throw new IllegalArgumentException("Column not found : " + columnName);
   }

   @Override
   public Class<?> getPrimitivePropertyType(RowPropertyHelper.RowMetadata entityType, String[] propertyPath) {
      // entityType is ignored in this case!

      Class<?> propType = getColumnAccessor(propertyPath).getPropertyType();
      if (propType.isEnum() || primitives.containsKey(propType)) {
         return propType;
      }
      return null;
   }

   private ColumnMetadata getColumnAccessor(String[] propertyPath) {
      if (propertyPath.length > 1) {
         throw new IllegalStateException("Nested attributes are not supported");
      }

      String columnName = propertyPath[0];
      for (RowPropertyHelper.ColumnMetadata c : rowMetadata.getColumns()) {
         if (c.getColumnName().equals(columnName)) {
            return c;
         }
      }

      throw new IllegalArgumentException("Column not found : " + columnName);
   }

   @Override
   public boolean hasProperty(RowPropertyHelper.RowMetadata entityType, String[] propertyPath) {
      if (propertyPath.length > 1) {
         throw new IllegalStateException("Nested attributes are not supported");
      }

      String columnName = propertyPath[0];
      for (RowPropertyHelper.ColumnMetadata c : rowMetadata.getColumns()) {
         if (c.getColumnName().equals(columnName)) {
            return true;
         }
      }
      return false;
   }

   @Override
   public boolean hasEmbeddedProperty(RowPropertyHelper.RowMetadata entityType, String[] propertyPath) {
      return false;
   }

   @Override
   public boolean isRepeatedProperty(RowPropertyHelper.RowMetadata entityType, String[] propertyPath) {
      return false;
   }
}
