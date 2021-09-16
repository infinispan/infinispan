package org.infinispan.configuration.module;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.cache.AbstractModuleConfigurationBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;

/**
 *
 * MyModuleConfigurationBuilder. A builder for {@link MyModuleConfiguration}
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class MyModuleConfigurationBuilder extends AbstractModuleConfigurationBuilder implements Builder<MyModuleConfiguration> {
   private String attribute;

   public MyModuleConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   public MyModuleConfigurationBuilder attribute(String attribute) {
      this.attribute = attribute;
      return this;
   }

   @Override
   public MyModuleConfiguration create() {
      return new MyModuleConfiguration(attribute);
   }

   @Override
   public MyModuleConfigurationBuilder read(MyModuleConfiguration template) {
      this.attribute(template.attribute());
      return this;
   }
}
