package org.infinispan.security.impl;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractModuleConfigurationBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public class CreatePermissionConfigurationBuilder extends AbstractModuleConfigurationBuilder implements Builder<CreatePermissionConfiguration>  {
   public CreatePermissionConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public AttributeSet attributes() {
      return null;
   }

   @Override
   public CreatePermissionConfiguration create() {
      return new CreatePermissionConfiguration();
   }

   @Override
   public Builder<?> read(CreatePermissionConfiguration template, Combine combine) {
      return this;
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }
}
