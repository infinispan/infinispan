package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
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
   static final AttributeDefinition<Long> REMOTE_CALL_TIMEOUT = AttributeDefinition.builder("remoteCallTimeout", TimeUnit.SECONDS.toMillis(15)).immutable().build();
   public static final AttributeSet attributeSet() {
      return new AttributeSet(ClusterLoaderConfiguration.class, AbstractStoreConfiguration.attributeSet(), REMOTE_CALL_TIMEOUT);
   }

   public ClusterLoaderConfiguration(AttributeSet attributes, AsyncStoreConfiguration async,
         SingletonStoreConfiguration singletonStore) {
      super(attributes, async, singletonStore);
   }

   public long remoteCallTimeout() {
      return attributes.attribute(REMOTE_CALL_TIMEOUT).asLong();
   }

   @Override
   public String toString() {
      return "ClusterLoaderConfiguration [attributes=" + attributes + "]";
   }

}
