package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.Matchable;

/**
 * Configures query options and defaults
 */
public class QueryConfiguration extends AbstractTypedPropertiesConfiguration implements Matchable<QueryConfiguration> {

   public static final AttributeDefinition<Integer> DEFAULT_MAX_RESULTS = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.DEFAULT_MAX_RESULTS, 100).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(QueryConfiguration.class, AbstractTypedPropertiesConfiguration.attributeSet(), DEFAULT_MAX_RESULTS);
   }

   protected QueryConfiguration(AttributeSet attributes) {
      super(attributes);
   }

   /**
    * Limits the number of results returned by a query. Applies to indexed, non-indexed, and hybrid queries.
    * Setting the default-max-results significantly improves performance of queries that don't have an explicit limit set.
    */
   public int defaultMaxResults() {
      return attributes.attribute(DEFAULT_MAX_RESULTS).get();
   }

   public AttributeSet attributes() {
      return attributes;
   }
}
