package org.infinispan.objectfilter.impl.hql;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class RowPropertyHelper extends ObjectPropertyHelper<RowPropertyHelper.RowMetadata> {

   public static final class RowMetadata {

      private final ColumnMetadata[] columns;

      public RowMetadata(ColumnMetadata[] columns) {
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

   private static final Set<Class<?>> primitives = new HashSet<Class<?>>();

   static {
      primitives.add(java.util.Date.class);
      primitives.add(String.class);
      primitives.add(Character.class);
      primitives.add(char.class);
      primitives.add(Double.class);
      primitives.add(double.class);
      primitives.add(Float.class);
      primitives.add(float.class);
      primitives.add(Long.class);
      primitives.add(long.class);
      primitives.add(Integer.class);
      primitives.add(int.class);
      primitives.add(Short.class);
      primitives.add(short.class);
      primitives.add(Byte.class);
      primitives.add(byte.class);
      primitives.add(Boolean.class);
      primitives.add(boolean.class);
   }

   private final RowMetadata rowMetadata;

   public RowPropertyHelper(RowMetadata rowMetadata) {
      super(null);
      this.rowMetadata = rowMetadata;
   }

   @Override
   public RowMetadata getEntityMetadata(String targetTypeName) {
      return rowMetadata;
   }

   @Override
   public Class<?> getPrimitivePropertyType(String entityType, List<String> propertyPath) {
      // entityType is ignored

      Class<?> propType = getColumnAccessor(propertyPath).getPropertyType();
      if (propType.isEnum() || primitives.contains(propType)) {
         return propType;
      }
      return null;
   }

   private ColumnMetadata getColumnAccessor(List<String> propertyPath) {
      if (propertyPath.size() > 1) {
         throw new IllegalStateException("Nested attributes are not supported");
      }

      String columnName = propertyPath.get(0);
      for (RowPropertyHelper.ColumnMetadata c : rowMetadata.getColumns()) {
         if (c.getColumnName().equals(columnName)) {
            return c;
         }
      }

      throw new IllegalArgumentException("Column not found : " + columnName);
   }

   @Override
   public boolean hasProperty(String entityType, List<String> propertyPath) {
      if (propertyPath.size() > 1) {
         throw new IllegalStateException("Nested attributes are not supported");
      }

      String columnName = propertyPath.get(0);
      for (RowPropertyHelper.ColumnMetadata c : rowMetadata.getColumns()) {
         if (c.getColumnName().equals(columnName)) {
            return true;
         }
      }
      return false;
   }

   @Override
   public boolean hasEmbeddedProperty(String entityType, List<String> propertyPath) {
      return false;
   }

   @Override
   public boolean isRepeatedProperty(String entityType, List<String> propertyPath) {
      return false;
   }
}
