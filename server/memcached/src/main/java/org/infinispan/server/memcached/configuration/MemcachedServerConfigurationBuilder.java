package org.infinispan.server.memcached.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.server.core.admin.AdminOperationsHandler;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;

/**
 * MemcachedServerConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class MemcachedServerConfigurationBuilder extends ProtocolServerConfigurationBuilder<MemcachedServerConfiguration, MemcachedServerConfigurationBuilder> implements
      Builder<MemcachedServerConfiguration> {

   public MemcachedServerConfigurationBuilder() {
      super(MemcachedServerConfiguration.DEFAULT_MEMCACHED_PORT, MemcachedServerConfiguration.attributeDefinitionSet());
      this.defaultCacheName(MemcachedServerConfiguration.DEFAULT_MEMCACHED_CACHE);
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
      attributes.attribute(MemcachedServerConfiguration.CLIENT_ENCODING).set(payloadType);
      return this;
   }

   @Override
   public MemcachedServerConfiguration create() {
      return new MemcachedServerConfiguration(attributes.protect(), ssl.create(), ipFilter.create());
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
   public Builder<?> read(MemcachedServerConfiguration template) {
      super.read(template);
      return this;
   }
}
