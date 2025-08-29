package org.infinispan.server.core.query.impl.indexing;

public class IndexingKeyMetadata {

   private String fieldName;
   private String typeFullName;
   private Integer includeDepth;

   public IndexingKeyMetadata(String fieldName, String typeFullName, Integer includeDepth) {
      this.fieldName = fieldName;
      this.typeFullName = typeFullName;
      this.includeDepth = includeDepth;
   }

   public String fieldName() {
      return fieldName;
   }

   public String typeFullName() {
      return typeFullName;
   }

   public Integer includeDepth() {
      return includeDepth;
   }

   @Override
   public String toString() {
      return "IndexingKeyMetadata{" +
            "fieldName='" + fieldName + '\'' +
            ", typeFullName='" + typeFullName + '\'' +
            ", includeDepth=" + includeDepth +
            '}';
   }
}
