package org.infinispan.server.core.query.impl.indexing;

import org.infinispan.api.annotations.indexing.option.Structure;
import org.infinispan.api.annotations.indexing.option.TermVector;
import org.infinispan.api.annotations.indexing.option.VectorSimilarity;
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

   private final FieldDescriptor fieldDescriptor;
   private final String name;
   private final boolean searchable;
   private final boolean projectable;
   private final boolean aggregable;
   private final boolean sortable;
   private final String indexNullAs;
   private final String analyzer;
   private final String normalizer;
   private final String searchAnalyzer;
   private final Boolean norms;
   private final TermVector termVector;
   private final Integer decimalScale;
   private final Integer dimension;
   private final VectorSimilarity similarity;
   private final Integer beamWidth;
   private final Integer maxConnection;
   private final Integer includeDepth;
   private final Structure structure;

   /**
    * Indicates if lazy initialization of {@link #indexNullAsObj}.
    */
   private volatile boolean isInitialized = false;
   private Object indexNullAsObj;

   private FieldMapping(FieldDescriptor fieldDescriptor, String name,
                       boolean searchable, boolean projectable, boolean aggregable, boolean sortable,
                       String indexNullAs, String analyzer, String normalizer,
                       String searchAnalyzer, Boolean norms, TermVector termVector, Integer decimalScale,
                       Integer dimension, VectorSimilarity similarity, Integer beamWidth, Integer maxConnection,
                       Integer includeDepth, Structure structure) {
      if (name == null) {
         throw new IllegalArgumentException("name argument cannot be null");
      }
      if (fieldDescriptor == null) {
         throw new IllegalArgumentException("fieldDescriptor argument cannot be null");
      }
      this.fieldDescriptor = fieldDescriptor;
      this.name = name;
      this.searchable = searchable;
      this.projectable = projectable;
      this.aggregable = aggregable;
      this.sortable = sortable;
      this.indexNullAs = indexNullAs;
      this.analyzer = analyzer;
      this.normalizer = normalizer;
      this.searchAnalyzer = searchAnalyzer;
      this.norms = norms;
      this.termVector = termVector;
      this.decimalScale = decimalScale;
      this.dimension = dimension;
      this.similarity = similarity;
      this.beamWidth = beamWidth;
      this.maxConnection = maxConnection;
      this.includeDepth = includeDepth;
      this.structure = structure;
   }

   public static FieldMapping.Builder make(FieldDescriptor fieldDescriptor, String name,
                                           boolean searchable, boolean projectable, boolean aggregable, boolean sortable) {
      return new FieldMapping.Builder(fieldDescriptor, name, searchable, projectable, aggregable, sortable);
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

   public boolean normalized() {
      return normalizer != null;
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

   public Integer dimension() {
      return dimension;
   }

   public VectorSimilarity similarity() {
      return similarity;
   }

   public Integer beamWidth() {
      return beamWidth;
   }

   public Integer maxConnection() {
      return maxConnection;
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
            "fieldDescriptor=" + fieldDescriptor +
            ", name='" + name + '\'' +
            ", searchable=" + searchable +
            ", projectable=" + projectable +
            ", aggregable=" + aggregable +
            ", sortable=" + sortable +
            ", indexNullAs='" + indexNullAs + '\'' +
            ", analyzer='" + analyzer + '\'' +
            ", normalizer='" + normalizer + '\'' +
            ", searchAnalyzer='" + searchAnalyzer + '\'' +
            ", norms=" + norms +
            ", termVector=" + termVector +
            ", decimalScale=" + decimalScale +
            ", dimension=" + dimension +
            ", similarity=" + similarity +
            ", beamWidth=" + beamWidth +
            ", maxConnection=" + maxConnection +
            ", includeDepth=" + includeDepth +
            ", structure=" + structure +
            '}';
   }

   public static class Builder {
      private final FieldDescriptor fieldDescriptor;
      private final String name;
      private final boolean searchable;
      private final boolean projectable;
      private final boolean aggregable;
      private final boolean sortable;

      private String indexNullAs;
      private String analyzer;
      private String normalizer;
      private String searchAnalyzer;
      private Boolean norms;
      private TermVector termVector;
      private Integer decimalScale;
      private Integer dimension;
      private VectorSimilarity similarity;
      private Integer beamWidth;
      private Integer maxConnection;
      private Integer includeDepth;
      private Structure structure;

      private Builder(FieldDescriptor fieldDescriptor, String name,
                      boolean searchable, boolean projectable, boolean aggregable, boolean sortable) {
         this.fieldDescriptor = fieldDescriptor;
         this.name = name;
         this.searchable = searchable;
         this.projectable = projectable;
         this.aggregable = aggregable;
         this.sortable = sortable;
         this.indexNullAs = indexNullAs;
      }

      public Builder indexNullAs(String indexNullAs) {
         this.indexNullAs = indexNullAs;
         return this;
      }

      public Builder analyzer(String analyzer) {
         this.analyzer = analyzer;
         return this;
      }

      public Builder keyword(String normalizer, boolean norms) {
         this.normalizer = normalizer;
         this.norms = norms;
         return this;
      }

      public Builder text(String analyzer, String searchAnalyzer, boolean norms, TermVector termVector) {
         this.analyzer = analyzer;
         this.searchAnalyzer = searchAnalyzer;
         this.norms = norms;
         this.termVector = termVector;
         return this;
      }

      public Builder decimalScale(int decimalScale) {
         this.decimalScale = decimalScale;
         return this;
      }

      public Builder vector(int dimension, VectorSimilarity similarity, int beamWidth, int maxConnection) {
         this.dimension = dimension;
         this.similarity = similarity;
         this.beamWidth = beamWidth;
         this.maxConnection = maxConnection;
         return this;
      }

      public Builder embedded(int includeDepth, Structure structure) {
         this.includeDepth = includeDepth;
         this.structure = structure;
         return this;
      }

      public FieldMapping build() {
         return new FieldMapping(fieldDescriptor, name, searchable, projectable, aggregable, sortable, indexNullAs,
               analyzer, normalizer, searchAnalyzer, norms, termVector, decimalScale,
               dimension, similarity, beamWidth, maxConnection, includeDepth, structure);
      }
   }
}
