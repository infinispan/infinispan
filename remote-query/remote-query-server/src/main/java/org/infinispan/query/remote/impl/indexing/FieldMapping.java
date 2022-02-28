package org.infinispan.query.remote.impl.indexing;

import org.infinispan.protostream.descriptors.EnumValueDescriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;

/**
 * A mapping from an object field to an index field and the flags that enable indexing, storage and analysis.
 * This is used only for non-spatial fields.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
public final class FieldMapping {

   /**
    * The name of the field in the index.
    */
   private final String name;

   /**
    * Enable indexing.
    */
   private final boolean index;

   /**
    * Enable analysis.
    */
   private final boolean analyze;

   /**
    * Enable storage.
    */
   private final boolean store;

   private final boolean sortable;

   /**
    * The name of the analyzer definition.
    */
   private final String analyzer;

   private final String indexNullAs;

   private final FieldDescriptor fieldDescriptor;

   /**
    * Indicates if lazy initialization of {@link #indexNullAsObj}.
    */
   private volatile boolean isInitialized = false;

   private Object indexNullAsObj;

   FieldMapping(String name, boolean index, boolean analyze, boolean store, boolean sortable, String analyzer,
                String indexNullAs,
                FieldDescriptor fieldDescriptor) {
      if (name == null) {
         throw new IllegalArgumentException("name argument cannot be null");
      }
      if (fieldDescriptor == null) {
         throw new IllegalArgumentException("fieldDescriptor argument cannot be null");
      }
      this.name = name;
      this.index = index;
      this.analyze = analyze;
      this.store = store;
      this.sortable = sortable;
      this.analyzer = analyzer;
      this.indexNullAs = indexNullAs;
      this.fieldDescriptor = fieldDescriptor;
   }

   public String name() {
      return name;
   }

   public boolean index() {
      return index;
   }

   public boolean analyze() {
      return analyze;
   }

   public boolean store() {
      return store;
   }

   public boolean sortable() {
      return sortable;
   }

   public String analyzer() {
      return analyzer;
   }

   public Object indexNullAs() {
      init();
      return indexNullAsObj;
   }

   public String notParsedIndexNull() {
      return indexNullAs;
   }

   private void init() {
      if (!isInitialized) {
         if (fieldDescriptor.getType() == null) {
            // this could only happen due to a programming error
            throw new IllegalStateException("FieldDescriptor not fully initialised!");
         }
         indexNullAsObj = parseIndexNullAs();
         isInitialized = true;
      }
   }

   public Object parseIndexNullAs() {
      if (indexNullAs != null) {
         switch (fieldDescriptor.getType()) {
            case DOUBLE:
               return Double.parseDouble(indexNullAs);
            case FLOAT:
               return Float.parseFloat(indexNullAs);
            case INT64:
            case UINT64:
            case FIXED64:
            case SFIXED64:
            case SINT64:
               return Long.parseLong(indexNullAs);
            case INT32:
            case FIXED32:
            case UINT32:
            case SFIXED32:
            case SINT32:
               return Integer.parseInt(indexNullAs);
            case ENUM:
               EnumValueDescriptor enumVal = fieldDescriptor.getEnumType().findValueByName(indexNullAs);
               if (enumVal == null) {
                  throw new IllegalArgumentException("Enum value not found : " + indexNullAs);
               }
               return enumVal.getNumber();
            case BOOL:
               return Boolean.valueOf(indexNullAs);
         }
      }
      return indexNullAs;
   }

   public FieldDescriptor getFieldDescriptor() {
      return fieldDescriptor;
   }

   @Override
   public String toString() {
      return "FieldMapping{" +
            "name='" + name + '\'' +
            ", index=" + index +
            ", analyze=" + analyze +
            ", store=" + store +
            ", sortable=" + sortable +
            ", analyzer='" + analyzer + '\'' +
            ", indexNullAs=" + indexNullAs +
            ", fieldDescriptor=" + fieldDescriptor +
            '}';
   }
}
