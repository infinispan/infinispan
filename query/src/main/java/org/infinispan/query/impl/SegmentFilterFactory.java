package org.infinispan.query.impl;

import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;
import static org.infinispan.query.impl.SegmentFieldBridge.SEGMENT_FIELD;

import java.util.BitSet;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.hibernate.search.annotations.Factory;

/**
 * Filter for infinispan segments.
 *
 * @since 10.1
 */
public class SegmentFilterFactory {

   public static final String SEGMENT_FILTER_NAME = "segmentFilter";
   public static final String SEGMENT_PARAMETERS_NAME = "segments";

   private BitSet segments;

   @Factory
   public Query create() {
      if (segments == null) throw new IllegalStateException("Cannot filter, missing segments parameters");
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      segments.stream().forEach(seg -> builder.add(new TermQuery(new Term(SEGMENT_FIELD, String.valueOf(seg))), SHOULD));
      return builder.build();
   }

   public void setSegments(BitSet segments) {
      this.segments = segments;
   }

}
