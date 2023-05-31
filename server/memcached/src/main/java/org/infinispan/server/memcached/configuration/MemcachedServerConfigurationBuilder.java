package org.infinispan.server.memcached.configuration;

import static org.infinispan.server.memcached.configuration.MemcachedServerConfiguration.CLIENT_ENCODING;
import static org.infinispan.server.memcached.configuration.MemcachedServerConfiguration.DEFAULT_MEMCACHED_CACHE;
import static org.infinispan.server.memcached.configuration.MemcachedServerConfiguration.DEFAULT_MEMCACHED_PORT;
import static org.infinispan.server.memcached.configuration.MemcachedServerConfiguration.PROTOCOL;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.server.core.admin.AdminOperationsHandler;
import org.infinispan.server.core.configuration.EncryptionConfigurationBuilder;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;

/**
 * MemcachedServerConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class MemcachedServerConfigurationBuilder extends ProtocolServerConfigurationBuilder<MemcachedServerConfiguration, MemcachedServerConfigurationBuilder, MemcachedAuthenticationConfiguration> implements
      Builder<MemcachedServerConfiguration> {
   private final MemcachedAuthenticationConfigurationBuilder authentication = new MemcachedAuthenticationConfigurationBuilder(this);
   private final EncryptionConfigurationBuilder encryption = new EncryptionConfigurationBuilder(ssl());

   public MemcachedServerConfigurationBuilder() {
      super(DEFAULT_MEMCACHED_PORT, MemcachedServerConfiguration.attributeDefinitionSet());
      this.defaultCacheName(DEFAULT_MEMCACHED_CACHE);
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public MemcachedServerConfigurationBuilder self() {
      return this;
   }

   /**
    * Use {@link ProtocolServerConfigurationBuilder#defaultCacheName(String)} instead
    */
   @Deprecated
   public MemcachedServerConfigurationBuilder cache(String cache) {
      this.defaultCacheName(cache);
      return this;
   }

   @Override
   public MemcachedAuthenticationConfigurationBuilder authentication() {
      return authentication;
   }

   public EncryptionConfigurationBuilder encryption() {
      return encryption;
   }

   @Override
   public MemcachedServerConfigurationBuilder adminOperationsHandler(AdminOperationsHandler handler) {
      // Ignore
      return this;
   }

   /**
    * The encoding to be used by clients of the memcached text protocol. When not specified, "application/octet-stream" is assumed.
    * When encoding is set, the memcached text server will assume clients will be reading and writing values in that encoding, and
    * will perform the necessary conversions between this encoding and the storage format.
    */
   public MemcachedServerConfigurationBuilder clientEncoding(MediaType payloadType) {
      attributes.attribute(CLIENT_ENCODING).set(payloadType);
      return this;
   }

   public MemcachedServerConfigurationBuilder protocol(MemcachedProtocol protocol) {
      attributes.attribute(PROTOCOL).set(protocol);
      return this;
   }

   public MemcachedProtocol protocol() {
      return attributes.attribute(PROTOCOL).get();
   }

   @Override
   public MemcachedServerConfiguration create() {
      return new MemcachedServerConfiguration(attributes.protect(), authentication().create(), ssl.create(), encryption.create(), ipFilter.create());
   }

   @Override
   public void validate() {
      super.validate();
      authentication.validate();
      encryption.validate();
   }

   public MemcachedServerConfiguration build(boolean validate) {
      if (validate) {
         validate();
      }
      return create();
   }

   @Override
   public MemcachedServerConfiguration build() {
      return build(true);
   }

   @Override
   public Builder<?> read(MemcachedServerConfiguration template, Combine combine) {
      this.authentication.read(template.authentication(), combine);
      this.encryption.read(template.encryption(), combine);
      return this;
   }
}
