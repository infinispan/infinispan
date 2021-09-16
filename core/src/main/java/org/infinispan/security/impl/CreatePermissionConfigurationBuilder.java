package org.infinispan.security.impl;

import org.infinispan.commons.configuration.Builder;
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
   public CreatePermissionConfiguration create() {
      return new CreatePermissionConfiguration();
   }

   @Override
   public Builder<?> read(CreatePermissionConfiguration template) {
      return this;
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }
}
