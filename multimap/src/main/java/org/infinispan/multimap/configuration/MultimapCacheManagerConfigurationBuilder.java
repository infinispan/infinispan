package org.infinispan.multimap.configuration;

import static org.infinispan.multimap.logging.Log.CONTAINER;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;

public class MultimapCacheManagerConfigurationBuilder implements Builder<MultimapCacheManagerConfiguration> {

   private final AttributeSet attributes = MultimapCacheManagerConfiguration.attributeDefinitions();
   private final List<EmbeddedMultimapConfigurationBuilder> multimaps;
   private final GlobalConfigurationBuilder builder;

   public MultimapCacheManagerConfigurationBuilder(GlobalConfigurationBuilder builder) {
      this.builder = builder;
      this.multimaps = new ArrayList<>();
   }

   public MultimapCacheManagerConfigurationBuilder(ConfigurationBuilder ignore) {
      this((GlobalConfigurationBuilder) null);
   }

   @Override
   public void validate() {
      multimaps.forEach(EmbeddedMultimapConfigurationBuilder::validate);
      Set<String> names = new HashSet<>();
      for (EmbeddedMultimapConfigurationBuilder builder : multimaps) {
         if (!names.add(builder.name())) {
            throw CONTAINER.duplicatedMultimapName(builder.name());
         }
      }
   }

   @Override
   public MultimapCacheManagerConfiguration create() {
      Map<String, EmbeddedMultimapConfiguration> configurations = new HashMap<>(multimaps.size());
      for (EmbeddedMultimapConfigurationBuilder builder : multimaps) {
         if (configurations.put(builder.name(), builder.create()) != null) {
            throw CONTAINER.duplicatedMultimapName(builder.name());
         }
      }
      return new MultimapCacheManagerConfiguration(attributes.protect(), configurations);
   }

   @Override
   public Builder<MultimapCacheManagerConfiguration> read(MultimapCacheManagerConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public EmbeddedMultimapConfigurationBuilder addMultimap() {
      EmbeddedMultimapConfigurationBuilder mb = new EmbeddedMultimapConfigurationBuilder();
      multimaps.add(mb);
      return mb;
   }
}
