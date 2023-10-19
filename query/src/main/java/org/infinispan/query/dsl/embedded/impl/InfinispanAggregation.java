package org.infinispan.query.dsl.embedded.impl;

import java.util.Map;

import org.hibernate.search.engine.search.aggregation.SearchAggregation;
import org.infinispan.objectfilter.impl.syntax.parser.AggregationPropertyPath;

public class InfinispanAggregation<T> {

   private final SearchAggregation<Map<T, Long>> searchAggregation;
   private final AggregationPropertyPath propertyPath;
   private final boolean displayGroupFirst;

   public InfinispanAggregation(SearchAggregation<Map<T, Long>> searchAggregation, AggregationPropertyPath propertyPath,
                                boolean displayGroupFirst) {
      this.searchAggregation = searchAggregation;
      this.propertyPath = propertyPath;
      this.displayGroupFirst = displayGroupFirst;
   }

   public SearchAggregation<Map<T, Long>> searchAggregation() {
      return searchAggregation;
   }

   public AggregationPropertyPath propertyPath() {
      return propertyPath;
   }

   public boolean displayGroupFirst() {
      return displayGroupFirst;
   }
}
