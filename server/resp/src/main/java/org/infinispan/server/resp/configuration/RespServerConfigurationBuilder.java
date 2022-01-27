package org.infinispan.server.resp.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.server.core.admin.AdminOperationsHandler;
import org.infinispan.server.core.configuration.EncryptionConfigurationBuilder;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;
import org.infinispan.server.resp.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * RespServerConfigurationBuilder.
 *
 * @author William Burns
 * @since 14.0
 */
public class RespServerConfigurationBuilder extends ProtocolServerConfigurationBuilder<RespServerConfiguration, RespServerConfigurationBuilder> implements
      Builder<RespServerConfiguration> {
   final static Log logger = LogFactory.getLog(RespServerConfigurationBuilder.class, Log.class);

   private final AuthenticationConfigurationBuilder authentication =  new AuthenticationConfigurationBuilder(this);
   private final EncryptionConfigurationBuilder encryption = new EncryptionConfigurationBuilder(ssl());

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

   public AuthenticationConfigurationBuilder authentication() {
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
   public Builder<?> read(RespServerConfiguration template) {
      super.read(template);
      this.authentication.read(template.authentication());
      this.encryption.read(template.encryption());
      return this;
   }
}
