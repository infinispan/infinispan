package org.infinispan.server.memcached.configuration;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.server.core.configuration.EncryptionConfiguration;
import org.infinispan.server.core.configuration.IpFilterConfiguration;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.SslConfiguration;
import org.infinispan.server.memcached.MemcachedServer;

/**
 * MemcachedServerConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
@BuiltBy(MemcachedServerConfigurationBuilder.class)
@ConfigurationFor(MemcachedServer.class)
public class MemcachedServerConfiguration extends ProtocolServerConfiguration<MemcachedServerConfiguration, MemcachedAuthenticationConfiguration> {

   public static final int DEFAULT_MEMCACHED_PORT = 11211;
   public static final String DEFAULT_MEMCACHED_CACHE = "memcachedCache";

   public static final AttributeDefinition<MediaType> CLIENT_ENCODING = AttributeDefinition.builder("client-encoding", APPLICATION_OCTET_STREAM, MediaType.class).immutable().build();
   public static final AttributeDefinition<MemcachedProtocol> PROTOCOL = AttributeDefinition.builder("protocol", MemcachedProtocol.AUTO, MemcachedProtocol.class).immutable().build();
   private final EncryptionConfiguration encryption;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(MemcachedServerConfiguration.class, ProtocolServerConfiguration.attributeDefinitionSet(), CLIENT_ENCODING, PROTOCOL);
   }

   MemcachedServerConfiguration(AttributeSet attributes, MemcachedAuthenticationConfiguration authentication, SslConfiguration ssl, EncryptionConfiguration encryptionConfiguration, IpFilterConfiguration ipRules) {
      super("memcached-connector", attributes, authentication, ssl, ipRules);
      this.encryption = encryptionConfiguration;
   }

   public EncryptionConfiguration encryption() {
      return encryption;
   }

   public MediaType clientEncoding() {
      return attributes.attribute(CLIENT_ENCODING).get();
   }

   public MemcachedProtocol protocol() {
      return attributes.attribute(PROTOCOL).get();
   }
}
