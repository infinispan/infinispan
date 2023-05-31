package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.IndexReaderConfiguration.REFRESH_INTERVAL;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 12.0
 */
public class IndexReaderConfigurationBuilder extends AbstractIndexingConfigurationChildBuilder
      implements Builder<IndexReaderConfiguration> {

   private final AttributeSet attributes;
   private final Attribute<Long> refreshInterval;

   IndexReaderConfigurationBuilder(IndexingConfigurationBuilder builder) {
      super(builder);
      this.attributes = IndexReaderConfiguration.attributeDefinitionSet();
      this.refreshInterval = attributes.attribute(REFRESH_INTERVAL);
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public IndexReaderConfigurationBuilder refreshInterval(long valueMillis) {
      refreshInterval.set(valueMillis);
      return this;
   }

   @Override
   public IndexReaderConfiguration create() {
      return new IndexReaderConfiguration(attributes.protect());
   }

   @Override
   public IndexReaderConfigurationBuilder read(IndexReaderConfiguration template, Combine combine) {
      this.attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public String toString() {
      return "IndexReaderConfigurationBuilder{" +
            "attributes=" + attributes +
            '}';
   }
}
