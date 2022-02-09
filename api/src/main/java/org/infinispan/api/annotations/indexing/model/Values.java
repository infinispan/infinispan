package org.infinispan.api.annotations.indexing.model;

import org.infinispan.api.annotations.indexing.Decimal;
import org.infinispan.api.annotations.indexing.Embedded;
import org.infinispan.api.annotations.indexing.Text;

public final class Values {

   /**
    * This special value is reserved to not index the null value, that is the default behaviour.
    */
   public static final String DO_NOT_INDEX_NULL = "__Infinispan_indexNullAs_doNotIndexNull";

   /**
    * The Infinispan default decimal scale for {@link Decimal#decimalScale()}
    */
   public static final int DEFAULT_DECIMAL_SCALE = 2;

   /**
    * The Infinispan default include depth for {@link Embedded#includeDepth()}
    */
   public static final int DEFAULT_EMBEDDED_INCLUDE_DEPTH = 3;

   /**
    * The Infinispan default analyzer for {@link Text#analyzer()}
    */
   public static final String DEFAULT_ANALYZER = "standard";

   private Values() {
   }

}
