package org.infinispan.objectfilter.impl;

import java.util.Map;

import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.impl.predicateindex.MatcherEvalContext;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;

/**
 * A filter that accepts all inputs of a given type. Does not support sorting and projections.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
final class AcceptObjectFilter<TypeMetadata, AttributeMetadata, AttributeId extends Comparable<AttributeId>>
      extends ObjectFilterBase<TypeMetadata> implements ObjectFilter {

   private final BaseMatcher<TypeMetadata, AttributeMetadata, AttributeId> matcher;

   private final MetadataAdapter<TypeMetadata, AttributeMetadata, AttributeId> metadataAdapter;

   AcceptObjectFilter(Map<String, Object> namedParameters,
                      BaseMatcher<TypeMetadata, AttributeMetadata, AttributeId> matcher,
                      MetadataAdapter<TypeMetadata, AttributeMetadata, AttributeId> metadataAdapter,
                      IckleParsingResult<TypeMetadata> parsingResult) {
      super(parsingResult, namedParameters);
      this.matcher = matcher;
      this.metadataAdapter = metadataAdapter;
   }

   @Override
   public ObjectFilter withParameters(Map<String, Object> namedParameters) {
      validateParameters(namedParameters);
      return new AcceptObjectFilter<>(namedParameters, matcher, metadataAdapter, parsingResult);
   }

   @Override
   public FilterResult filter(Object instance) {
      if (instance == null) {
         throw new IllegalArgumentException("instance cannot be null");
      }
      MatcherEvalContext<TypeMetadata, AttributeMetadata, AttributeId> matcherEvalContext = matcher.startSingleTypeContext(null, null, instance, metadataAdapter);
      if (matcherEvalContext != null) {
         // once we have a successfully created context we already have a match as there are no filter conditions except for entity type
         return new FilterResultImpl(matcher.convert(instance), null, null);
      }
      return null;
   }
}
