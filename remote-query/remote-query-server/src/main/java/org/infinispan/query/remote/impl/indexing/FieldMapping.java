package org.infinispan.query.remote.impl.indexing;

import org.infinispan.api.annotations.indexing.option.Structure;
import org.infinispan.api.annotations.indexing.option.TermVector;
import org.infinispan.protostream.descriptors.EnumValueDescriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;

/**
 * A mapping from an object field to an index field and the flags that enable indexing, storage and analysis.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
public final class FieldMapping {

   private final String name;
   private final FieldDescriptor fieldDescriptor;

   private final boolean searchable;
   private final boolean projectable;
   private final boolean aggregable;
   private final boolean sortable;
   private final String analyzer;
   private final String normalizer;
   private final String indexNullAs;
   private final Boolean norms;
   private final String searchAnalyzer;
   private final TermVector termVector;
   private final Integer decimalScale;
   private final Integer includeDepth;
   private final Structure structure;

   /**
    * Indicates if lazy initialization of {@link #indexNullAsObj}.
    */
   private volatile boolean isInitialized = false;

   private Object indexNullAsObj;

   public FieldMapping(String name, boolean searchable, boolean projectable, boolean aggregable, boolean sortable,
                       String analyzer, String normalizer, String indexNullAs, FieldDescriptor fieldDescriptor) {
      this(name, searchable, projectable, aggregable, sortable, analyzer, normalizer, indexNullAs,
            null, null, null, null, 3, null, fieldDescriptor);
   }

   public FieldMapping(String name, Boolean searchable, Boolean projectable, Boolean aggregable, Boolean sortable,
                       String analyzer, String normalizer, String indexNullAs,
                       Boolean norms, String searchAnalyzer, TermVector termVector, Integer decimalScale,
                       Integer includeDepth, Structure structure,
                       FieldDescriptor fieldDescriptor) {
      if (name == null) {
         throw new IllegalArgumentException("name argument cannot be null");
      }
      if (fieldDescriptor == null) {
         throw new IllegalArgumentException("fieldDescriptor argument cannot be null");
      }
      this.name = name;
      this.fieldDescriptor = fieldDescriptor;
      this.searchable = searchable;
      this.projectable = projectable;
      this.aggregable = aggregable;
      this.sortable = sortable;
      this.analyzer = analyzer;
      this.normalizer = normalizer;
      this.indexNullAs = indexNullAs;
      this.norms = norms;
      this.searchAnalyzer = searchAnalyzer;
      this.termVector = termVector;
      this.decimalScale = decimalScale;
      this.includeDepth = includeDepth;
      this.structure = structure;
   }

   public String name() {
      return name;
   }

   public boolean searchable() {
      return searchable;
   }

   public boolean projectable() {
      return projectable;
   }

   public boolean aggregable() {
      return aggregable;
   }

   public boolean sortable() {
      return sortable;
   }

   public String analyzer() {
      return analyzer;
   }

   public String normalizer() {
      return normalizer;
   }

   public boolean analyzed() {
      return analyzer != null;
   }

   public Object indexNullAs() {
      init();
      return indexNullAsObj;
   }

   public Boolean norms() {
      return norms;
   }

   public String searchAnalyzer() {
      return searchAnalyzer;
   }

   public TermVector termVector() {
      return termVector;
   }

   public Integer decimalScale() {
      return decimalScale;
   }

   public Integer includeDepth() {
      return includeDepth;
   }

   public Structure structure() {
      return structure;
   }

   private void init() {
      if (!isInitialized) {
         if (fieldDescriptor.getType() == null) {
            // this could only happen due to a programming error
            throw new IllegalStateException("FieldDescriptors are not fully initialised!");
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

   @Override
   public String toString() {
      return "FieldMapping{" +
            "name='" + name + '\'' +
            ", fieldDescriptor=" + fieldDescriptor +
            ", searchable=" + searchable +
            ", projectable=" + projectable +
            ", aggregable=" + aggregable +
            ", sortable=" + sortable +
            ", analyzer='" + analyzer + '\'' +
            ", normalizer='" + normalizer + '\'' +
            ", indexNullAs='" + indexNullAs + '\'' +
            ", norms=" + norms +
            ", searchAnalyzer='" + searchAnalyzer + '\'' +
            ", termVector=" + termVector +
            ", decimalScale=" + decimalScale +
            ", includeDepth=" + includeDepth +
            ", structure=" + structure +
            '}';
   }
}
