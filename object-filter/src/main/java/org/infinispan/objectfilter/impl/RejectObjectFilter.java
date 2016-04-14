package org.infinispan.objectfilter.impl;

import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.impl.hql.FilterParsingResult;

import java.util.Map;

/**
 * A filter that rejects all inputs. Does not support sorting and projections.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
final class RejectObjectFilter<TypeMetadata>
      extends ObjectFilterBase<TypeMetadata> implements ObjectFilter {

   RejectObjectFilter(Map<String, Object> namedParameters, FilterParsingResult<TypeMetadata> parsingResult) {
      super(parsingResult, namedParameters);
   }

   @Override
   public ObjectFilter withParameters(Map<String, Object> namedParameters) {
      validateParameters(namedParameters);
      return new RejectObjectFilter<>(namedParameters, parsingResult);
   }

   @Override
   public FilterResult filter(Object instance) {
      if (instance == null) {
         throw new IllegalArgumentException("instance cannot be null");
      }
      return null;
   }
}
