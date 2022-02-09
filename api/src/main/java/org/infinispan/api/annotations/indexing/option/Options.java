package org.infinispan.api.annotations.indexing.option;

import org.hibernate.search.util.common.AssertionFailure;
import org.infinispan.api.annotations.indexing.Decimal;
import org.infinispan.api.annotations.indexing.Embedded;
import org.infinispan.api.annotations.indexing.FullText;

public final class Options {

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
    * The Infinispan default analyzer for {@link FullText#analyzer()}
    */
   public static final String DEFAULT_ANALYZER = "standard";

   private Options() {
   }

   public static org.hibernate.search.engine.backend.types.Norms norms(Norms norms) {
      return (Norms.YES.equals(norms)) ?
            org.hibernate.search.engine.backend.types.Norms.YES :
            org.hibernate.search.engine.backend.types.Norms.NO;
   }

   public static org.hibernate.search.engine.backend.types.Sortable sortable(Sortable sortable) {
      return (Sortable.YES.equals(sortable)) ?
            org.hibernate.search.engine.backend.types.Sortable.YES :
            org.hibernate.search.engine.backend.types.Sortable.NO;
   }

   public static org.hibernate.search.engine.backend.types.Aggregable aggregable(Aggregable aggregable) {
      return (Aggregable.YES.equals(aggregable)) ?
            org.hibernate.search.engine.backend.types.Aggregable.YES :
            org.hibernate.search.engine.backend.types.Aggregable.NO;
   }

   public static org.hibernate.search.engine.backend.types.Projectable projectable(Projectable projectable) {
      return (Projectable.YES.equals(projectable)) ?
            org.hibernate.search.engine.backend.types.Projectable.YES :
            org.hibernate.search.engine.backend.types.Projectable.NO;
   }

   public static org.hibernate.search.engine.backend.types.Searchable searchable(Searchable searchable) {
      return (Searchable.YES.equals(searchable)) ?
            org.hibernate.search.engine.backend.types.Searchable.YES :
            org.hibernate.search.engine.backend.types.Searchable.NO;
   }

   public static org.hibernate.search.engine.backend.types.TermVector termVector(TermVector termVector) {
      switch (termVector) {
         case YES:
            return org.hibernate.search.engine.backend.types.TermVector.YES;
         case NO:
            return org.hibernate.search.engine.backend.types.TermVector.NO;
         case WITH_POSITIONS:
            return org.hibernate.search.engine.backend.types.TermVector.WITH_POSITIONS;
         case WITH_OFFSETS:
            return org.hibernate.search.engine.backend.types.TermVector.WITH_OFFSETS;
         case WITH_POSITIONS_OFFSETS:
            return org.hibernate.search.engine.backend.types.TermVector.WITH_POSITIONS_OFFSETS;
         case WITH_POSITIONS_PAYLOADS:
            return org.hibernate.search.engine.backend.types.TermVector.WITH_POSITIONS_PAYLOADS;
         case WITH_POSITIONS_OFFSETS_PAYLOADS:
            return org.hibernate.search.engine.backend.types.TermVector.WITH_POSITIONS_OFFSETS_PAYLOADS;
         default:
            throw new AssertionFailure("Unexpected value for TermVector: " + termVector);
      }
   }
}
