package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.persistence.cluster.ClusterLoader;

/**
 * ClusterLoaderConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.2
 * @deprecated since 11.0. To be removed in 14.0 ISPN-11864 with no direct replacement.
 */
@BuiltBy(ClusterLoaderConfigurationBuilder.class)
@ConfigurationFor(ClusterLoader.class)
@Deprecated
public class ClusterLoaderConfiguration extends AbstractStoreConfiguration {

   static final AttributeDefinition<Long> REMOTE_CALL_TIMEOUT = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.REMOTE_TIMEOUT, TimeUnit.SECONDS.toMillis(15)).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ClusterLoaderConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(), REMOTE_CALL_TIMEOUT);
   }

   private final Attribute<Long> remoteCallTimeout;

   ClusterLoaderConfiguration(AttributeSet attributes, AsyncStoreConfiguration async) {
      super(attributes, async);
      remoteCallTimeout = attributes.attribute(REMOTE_CALL_TIMEOUT);
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public long remoteCallTimeout() {
      return remoteCallTimeout.get();
   }

   @Override
   public String toString() {
      return "ClusterLoaderConfiguration [attributes=" + attributes + "]";
   }
}
