package org.infinispan.server.resp.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.server.core.admin.AdminOperationsHandler;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;

/**
 * RespServerConfigurationBuilder.
 *
 * @author William Burns
 * @since 14.0
 */
public class RespServerConfigurationBuilder extends ProtocolServerConfigurationBuilder<RespServerConfiguration, RespServerConfigurationBuilder> implements
      Builder<RespServerConfiguration> {

   public RespServerConfigurationBuilder() {
      super(RespServerConfiguration.DEFAULT_RESP_PORT, RespServerConfiguration.attributeDefinitionSet());
      this.defaultCacheName(RespServerConfiguration.DEFAULT_RESP_CACHE);
   }

   @Override
   public RespServerConfigurationBuilder self() {
      return this;
   }

   /**
    * Use {@link ProtocolServerConfigurationBuilder#defaultCacheName(String)} instead
    */
   @Deprecated
   public RespServerConfigurationBuilder cache(String cache) {
      this.defaultCacheName(cache);
      return this;
   }

   @Override
   public RespServerConfigurationBuilder adminOperationsHandler(AdminOperationsHandler handler) {
      // Ignore
      return this;
   }

   @Override
   public RespServerConfiguration create() {
      return new RespServerConfiguration(attributes.protect(), ssl.create(), ipFilter.create());
   }

   public RespServerConfiguration build(boolean validate) {
      if (validate) {
         validate();
      }
      return create();
   }

   @Override
   public RespServerConfiguration build() {
      return build(true);
   }

   @Override
   public Builder<?> read(RespServerConfiguration template) {
      super.read(template);
      return this;
   }
}
