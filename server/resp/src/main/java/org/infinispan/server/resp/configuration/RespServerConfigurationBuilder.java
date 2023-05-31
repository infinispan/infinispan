package org.infinispan.server.resp.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.core.admin.AdminOperationsHandler;
import org.infinispan.server.core.configuration.EncryptionConfigurationBuilder;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;

/**
 * RespServerConfigurationBuilder.
 *
 * @author William Burns
 * @since 14.0
 */
public class RespServerConfigurationBuilder extends ProtocolServerConfigurationBuilder<RespServerConfiguration, RespServerConfigurationBuilder, RespAuthenticationConfiguration> implements
      Builder<RespServerConfiguration> {

   private final RespAuthenticationConfigurationBuilder authentication =  new RespAuthenticationConfigurationBuilder(this);
   private final EncryptionConfigurationBuilder encryption = new EncryptionConfigurationBuilder(ssl());

   public RespServerConfigurationBuilder() {
      super(RespServerConfiguration.DEFAULT_RESP_PORT, RespServerConfiguration.attributeDefinitionSet());
      this.defaultCacheName(RespServerConfiguration.DEFAULT_RESP_CACHE);
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
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
   public RespAuthenticationConfigurationBuilder authentication() {
      return authentication;
   }

   public EncryptionConfigurationBuilder encryption() {
      return encryption;
   }

   @Override
   public RespServerConfiguration create() {
      return new RespServerConfiguration(attributes.protect(), ipFilter.create(), ssl.create(), authentication.create(), encryption.create());
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
   public Builder<?> read(RespServerConfiguration template, Combine combine) {
      super.read(template, combine);
      this.authentication.read(template.authentication(), combine);
      this.encryption.read(template.encryption(), combine);
      return this;
   }
}
