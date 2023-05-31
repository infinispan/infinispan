package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.QueryConfiguration.DEFAULT_MAX_RESULTS;
import static org.infinispan.configuration.cache.QueryConfiguration.HIT_COUNT_ACCURACY;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public class QueryConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<QueryConfiguration> {

   private final AttributeSet attributes;

   public QueryConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = QueryConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   /**
    * Limits the number of results returned by a query. Applies to indexed, non-indexed, and hybrid queries.
    * Setting the default-max-results significantly improves performance of queries that don't have an explicit limit set.
    *
    * @param defaultMaxResults The value to apply
    * @return <code>this</code>, for method chaining
    */
   public QueryConfigurationBuilder defaultMaxResults(int defaultMaxResults) {
      attributes.attribute(DEFAULT_MAX_RESULTS).set(defaultMaxResults);
      return this;
   }

   public int defaultMaxResults() {
      return attributes.attribute(DEFAULT_MAX_RESULTS).get();
   }

   /**
    * Limit the required accuracy of the hit count for the indexed queries to an upper-bound.
    * Setting the hit-count-accuracy could improve the performance of queries targeting large data sets.
    * For optimal performances set this value not much above the expected hit count.
    * If you do not require accurate hit counts, set it to a low value.
    *
    * @param hitCountAccuracy The value to apply
    * @return <code>this</code>, for method chaining
    */
   public QueryConfigurationBuilder hitCountAccuracy(int hitCountAccuracy) {
      attributes.attribute(HIT_COUNT_ACCURACY).set(hitCountAccuracy);
      return this;
   }

   public int hitCountAccuracy() {
      return attributes.attribute(HIT_COUNT_ACCURACY).get();
   }

   @Override
   public QueryConfiguration create() {
      return new QueryConfiguration(attributes.protect());
   }

   @Override
   public QueryConfigurationBuilder read(QueryConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }
}
