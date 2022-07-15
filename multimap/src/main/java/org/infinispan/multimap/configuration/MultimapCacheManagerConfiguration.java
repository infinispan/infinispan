package org.infinispan.multimap.configuration;

import java.util.Collections;
import java.util.Map;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.serializing.SerializedWith;

@BuiltBy(MultimapCacheManagerConfigurationBuilder.class)
@SerializedWith(MultimapCacheManagerConfigurationSerializer.class)
public class MultimapCacheManagerConfiguration extends ConfigurationElement<MultimapCacheManagerConfiguration> {

   private static final MultimapCacheManagerConfiguration DEFAULT = new MultimapCacheManagerConfigurationBuilder((GlobalConfigurationBuilder) null)
         .create();

   private final Map<String, EmbeddedMultimapConfiguration> multimaps;

   MultimapCacheManagerConfiguration(AttributeSet attributes, Map<String, EmbeddedMultimapConfiguration> multimaps) {
      super(Element.MULTIMAPS, attributes);
      this.multimaps = multimaps;
   }

   public Map<String, EmbeddedMultimapConfiguration> multimaps() {
      return Collections.unmodifiableMap(multimaps);
   }

   static AttributeSet attributeDefinitions() {
      return new AttributeSet(MultimapCacheManagerConfiguration.class);
   }

   public static MultimapCacheManagerConfiguration defaultConfiguration() {
      return DEFAULT;
   }
}
