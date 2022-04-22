package org.infinispan.api.annotations.indexing.model;

public final class Values {

   /**
    * This special value is reserved to not index the null value, that is the default behaviour.
    */
   public static final String DO_NOT_INDEX_NULL = "__Infinispan_indexNullAs_doNotIndexNull";

   private Values() {
   }

}
