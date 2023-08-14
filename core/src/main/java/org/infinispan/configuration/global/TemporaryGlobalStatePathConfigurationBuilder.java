package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.parsing.ParseUtils;

public class TemporaryGlobalStatePathConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<TemporaryGlobalStatePathConfiguration> {

   private final AttributeSet attributes;
   private String location;

   TemporaryGlobalStatePathConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      attributes = TemporaryGlobalStatePathConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public TemporaryGlobalStatePathConfigurationBuilder location(String path, String relativeTo) {
      attributes.attribute(GlobalStatePathConfiguration.PATH).set(path);
      attributes.attribute(GlobalStatePathConfiguration.RELATIVE_TO).set(relativeTo);
      this.location = ParseUtils.resolvePath(path, relativeTo);
      return this;
   }

   @Override
   public TemporaryGlobalStatePathConfiguration create() {
      return new TemporaryGlobalStatePathConfiguration(attributes.protect(), location);
   }

   @Override
   public TemporaryGlobalStatePathConfigurationBuilder read(TemporaryGlobalStatePathConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      location = template.getLocation();
      return this;
   }
}
