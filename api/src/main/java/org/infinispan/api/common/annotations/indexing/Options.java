package org.infinispan.api.common.annotations.indexing;

import org.hibernate.search.util.common.AssertionFailure;
import org.infinispan.api.annotations.indexing.option.Aggregable;
import org.infinispan.api.annotations.indexing.option.Norms;
import org.infinispan.api.annotations.indexing.option.Projectable;
import org.infinispan.api.annotations.indexing.option.Searchable;
import org.infinispan.api.annotations.indexing.option.Sortable;
import org.infinispan.api.annotations.indexing.option.TermVector;

public final class Options {

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
