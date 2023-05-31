package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.parsing.ParseUtils;

public class GlobalStatePathConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<GlobalStatePathConfiguration> {

   private final AttributeSet attributes;
   private String elementName;
   private String location;

   GlobalStatePathConfigurationBuilder(GlobalConfigurationBuilder globalConfig, String elementName) {
      super(globalConfig);
      this.elementName = elementName;
      attributes = GlobalStatePathConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public GlobalStatePathConfigurationBuilder location(String path, String relativeTo) {
      location = ParseUtils.resolvePath(path, relativeTo);
      attributes.attribute(GlobalStatePathConfiguration.PATH).set(path);
      attributes.attribute(GlobalStatePathConfiguration.RELATIVE_TO).set(relativeTo);
      return this;
   }

   public String getLocation() {
      return location;
   }

   @Override
   public GlobalStatePathConfiguration create() {
      return new GlobalStatePathConfiguration(attributes.protect(), elementName);
   }

   @Override
   public GlobalStatePathConfigurationBuilder read(GlobalStatePathConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      location = template.getLocation();
      elementName = template.elementName();
      return this;
   }
}
