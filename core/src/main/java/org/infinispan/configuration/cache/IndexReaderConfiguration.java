package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.util.TimeQuantity;
import org.infinispan.configuration.parsing.Element;

/**
 * @since 12.0
 */
public class IndexReaderConfiguration extends ConfigurationElement<IndexReaderConfiguration> {

   public static final AttributeDefinition<TimeQuantity> REFRESH_INTERVAL =
           AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.REFRESH_INTERVAL, TimeQuantity.valueOf(0)).parser(TimeQuantity.PARSER).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(IndexReaderConfiguration.class, REFRESH_INTERVAL);
   }

   private final Attribute<TimeQuantity> refreshInternal;

   IndexReaderConfiguration(AttributeSet attributes) {
      super(Element.INDEX_READER, attributes);
      this.refreshInternal = attributes.attribute(REFRESH_INTERVAL);
   }

   public Long getRefreshInterval() {
      return refreshInternal.get().longValue();
   }
}
