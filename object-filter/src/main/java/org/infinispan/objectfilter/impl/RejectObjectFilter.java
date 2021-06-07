package org.infinispan.objectfilter.impl;

import java.util.Map;

import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;

/**
 * A filter that rejects all input. Ignores sorting and projection.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
final class RejectObjectFilter<TypeMetadata> extends ObjectFilterBase<TypeMetadata> implements ObjectFilter {

   RejectObjectFilter(Map<String, Object> namedParameters, IckleParsingResult<TypeMetadata> parsingResult) {
      super(parsingResult, namedParameters);
   }

   @Override
   public ObjectFilter withParameters(Map<String, Object> namedParameters) {
      validateParameters(namedParameters);
      return new RejectObjectFilter<>(namedParameters, parsingResult);
   }

   @Override
   public FilterResult filter(Object key, Object value) {
      if (value == null) {
         throw new IllegalArgumentException("instance cannot be null");
      }
      return null;
   }
}
