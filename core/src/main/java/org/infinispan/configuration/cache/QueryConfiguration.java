package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Element;

/**
 * Configures query options and defaults
 */
public class QueryConfiguration extends ConfigurationElement<QueryConfiguration> {

   public static final AttributeDefinition<Integer> DEFAULT_MAX_RESULTS = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.DEFAULT_MAX_RESULTS, 100).immutable().build();
   public static final AttributeDefinition<Integer> HIT_COUNT_ACCURACY = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.HIT_COUNT_ACCURACY, 10_000).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(QueryConfiguration.class, AbstractTypedPropertiesConfiguration.attributeSet(), DEFAULT_MAX_RESULTS, HIT_COUNT_ACCURACY);
   }

   protected QueryConfiguration(AttributeSet attributes) {
      super(Element.QUERY, attributes);
   }

   /**
    * Limits the number of results returned by a query. Applies to indexed, non-indexed, and hybrid queries.
    * Setting the default-max-results significantly improves the performance of queries that don't have an explicit limit set.
    */
   public int defaultMaxResults() {
      return attributes.attribute(DEFAULT_MAX_RESULTS).get();
   }

   /**
    * Limit the required accuracy of the hit count for the indexed queries to an upper-bound.
    * Setting the hit-count-accuracy could improve the performance of queries targeting large data sets.
    * For optimal performances set this value not much above the expected hit count.
    * If you do not require accurate hit counts, set it to a low value.
    */
   public int hitCountAccuracy() {
      return attributes.attribute(HIT_COUNT_ACCURACY).get();
   }
}
