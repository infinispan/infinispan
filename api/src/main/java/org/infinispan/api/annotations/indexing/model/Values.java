package org.infinispan.api.annotations.indexing.model;

import org.infinispan.api.annotations.indexing.option.VectorSimilarity;

public final class Values {

   /**
    * This special value is reserved to not index the null value, that is the default behaviour.
    */
   public static final String DO_NOT_INDEX_NULL = "__Infinispan_indexNullAs_doNotIndexNull";

   public static final int DEFAULT_INCLUDE_DEPTH = 3;

   public static final VectorSimilarity DEFAULT_VECTOR_SIMILARITY = VectorSimilarity.L2;

   public static final int DEFAULT_BEAN_WIDTH = 512;

   public static final int DEFAULT_MAX_CONNECTIONS = 16;

   private Values() {
   }

}
