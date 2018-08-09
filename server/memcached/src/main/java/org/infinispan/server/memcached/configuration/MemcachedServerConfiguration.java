package org.infinispan.server.memcached.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.AttributeSet;
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

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(MemcachedServerConfiguration.class, ProtocolServerConfiguration.attributeDefinitionSet());
   }

   MemcachedServerConfiguration(AttributeSet attributes, SslConfiguration ssl) {
      super(attributes, ssl);
   }

   @Override
   public String toString() {
      return "MemcachedServerConfiguration [" + attributes + "]";
   }
}
