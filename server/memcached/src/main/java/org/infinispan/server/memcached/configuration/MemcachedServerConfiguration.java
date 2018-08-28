package org.infinispan.server.memcached.configuration;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.SslConfiguration;

/**
 * MemcachedServerConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
@BuiltBy(MemcachedServerConfigurationBuilder.class)
public class MemcachedServerConfiguration extends ProtocolServerConfiguration {

   public static final int DEFAULT_MEMCACHED_PORT = 11211;
   public static final String DEFAULT_MEMCACHED_CACHE = "memcachedCache";

   public static final AttributeDefinition<MediaType> CLIENT_ENCODING = AttributeDefinition.builder("client-encoding", APPLICATION_OCTET_STREAM, MediaType.class).immutable().build();
   private final Attribute<MediaType> clientEncoding;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(MemcachedServerConfiguration.class, ProtocolServerConfiguration.attributeDefinitionSet(), CLIENT_ENCODING);
   }

   MemcachedServerConfiguration(AttributeSet attributes, SslConfiguration ssl) {
      super(attributes, ssl);
      clientEncoding = attributes.attribute(CLIENT_ENCODING);
   }

   public MediaType clientEncoding() {
      return clientEncoding.get();
   }

   @Override
   public String toString() {
      return "MemcachedServerConfiguration [" + attributes + "]";
   }
}
