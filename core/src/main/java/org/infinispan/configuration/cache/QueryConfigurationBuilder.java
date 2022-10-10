package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.QueryConfiguration.DEFAULT_MAX_RESULTS;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public class QueryConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<QueryConfiguration> {

   private final AttributeSet attributes;

   public QueryConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = QueryConfiguration.attributeDefinitionSet();
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

   @Override
   public QueryConfiguration create() {
      return new QueryConfiguration(attributes.protect());
   }

   @Override
   public QueryConfigurationBuilder read(QueryConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }
}
