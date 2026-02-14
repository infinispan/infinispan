package org.infinispan.query.objectfilter.impl.syntax.parser;

import java.util.Collections;

import org.infinispan.query.objectfilter.impl.ql.AggregationFunction;
import org.infinispan.query.objectfilter.impl.syntax.parser.projection.CacheValuePropertyPath;

public class CacheValueAggregationPropertyPath<TypeDescriptor> extends AggregationPropertyPath<TypeDescriptor> {

   CacheValueAggregationPropertyPath() {
      super(AggregationFunction.COUNT, Collections.singletonList(
            new PropertyReference<>(CacheValuePropertyPath.VALUE_PROPERTY_NAME, null, true)));
   }
}
