package org.infinispan.configuration.cache;

import static org.infinispan.configuration.parsing.Element.CLUSTER_LOADER;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.persistence.cluster.ClusterLoader;

/**
 * ClusterLoaderConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@BuiltBy(ClusterLoaderConfigurationBuilder.class)
@ConfigurationFor(ClusterLoader.class)
public class ClusterLoaderConfiguration extends AbstractStoreConfiguration implements ConfigurationInfo {
   public static final AttributeDefinition<Long> REMOTE_CALL_TIMEOUT = AttributeDefinition.builder("remoteCallTimeout", TimeUnit.SECONDS.toMillis(15)).immutable().xmlName("remote-timeout").build();

   public static final AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ClusterLoaderConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(), REMOTE_CALL_TIMEOUT);
   }

   public static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(CLUSTER_LOADER.getLocalName());

   private final Attribute<Long> remoteCallTimeout;

   public ClusterLoaderConfiguration(AttributeSet attributes, AsyncStoreConfiguration async,
         SingletonStoreConfiguration singletonStore) {
      super(attributes, async, singletonStore);
      remoteCallTimeout = attributes.attribute(REMOTE_CALL_TIMEOUT);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
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
