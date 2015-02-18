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
 */
@BuiltBy(ClusterLoaderConfigurationBuilder.class)
@ConfigurationFor(ClusterLoader.class)
public class ClusterLoaderConfiguration extends AbstractStoreConfiguration {
   public static final AttributeDefinition<Long> REMOTE_CALL_TIMEOUT = AttributeDefinition.builder("remoteCallTimeout", TimeUnit.SECONDS.toMillis(15)).immutable().build();

   public static final AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ClusterLoaderConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(), REMOTE_CALL_TIMEOUT);
   }

   private final Attribute<Long> remoteCallTimeout;

   public ClusterLoaderConfiguration(AttributeSet attributes, AsyncStoreConfiguration async,
         SingletonStoreConfiguration singletonStore) {
      super(attributes, async, singletonStore);
      remoteCallTimeout = attributes.attribute(REMOTE_CALL_TIMEOUT);
   }

   public long remoteCallTimeout() {
      return remoteCallTimeout.get();
   }

   @Override
   public String toString() {
      return "ClusterLoaderConfiguration [attributes=" + attributes + "]";
   }

}
