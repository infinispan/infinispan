package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Element;

/**
 * @since 12.0
 */
public class IndexReaderConfiguration extends ConfigurationElement<IndexReaderConfiguration> {

   public static final AttributeDefinition<Long> REFRESH_INTERVAL =
         AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.REFRESH_INTERVAL, 0L, Long.class).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(IndexReaderConfiguration.class, REFRESH_INTERVAL);
   }

   private final Attribute<Long> refreshInternal;

   IndexReaderConfiguration(AttributeSet attributes) {
      super(Element.INDEX_READER, attributes);
      this.refreshInternal = attributes.attribute(REFRESH_INTERVAL);
   }

   public long getRefreshInterval() {
      return refreshInternal.get();
   }
}
