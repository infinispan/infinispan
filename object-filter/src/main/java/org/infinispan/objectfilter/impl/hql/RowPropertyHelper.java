package org.infinispan.objectfilter.impl.hql;

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
   public Class<?> getPrimitivePropertyType(String entityType, String[] propertyPath) {
      // entityType is ignored

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
   public boolean hasProperty(String entityType, String[] propertyPath) {
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
   public boolean hasEmbeddedProperty(String entityType, String[] propertyPath) {
      return false;
   }

   @Override
   public boolean isRepeatedProperty(String entityType, String[] propertyPath) {
      return false;
   }
}
