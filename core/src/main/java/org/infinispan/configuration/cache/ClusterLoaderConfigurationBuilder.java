package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.AbstractStoreConfiguration.PROPERTIES;
import static org.infinispan.configuration.cache.ClusterLoaderConfiguration.REMOTE_CALL_TIMEOUT;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.parsing.XmlConfigHelper;

public class ClusterLoaderConfigurationBuilder extends AbstractStoreConfigurationBuilder<ClusterLoaderConfiguration, ClusterLoaderConfigurationBuilder> {

   public ClusterLoaderConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, ClusterLoaderConfiguration.attributeDefinitionSet());
   }

   @Override
   public ClusterLoaderConfigurationBuilder self() {
      return this;
   }

   public ClusterLoaderConfigurationBuilder remoteCallTimeout(long remoteCallTimeout) {
      attributes.attribute(REMOTE_CALL_TIMEOUT).set(remoteCallTimeout);
      return this;
   }

   public ClusterLoaderConfigurationBuilder remoteCallTimeout(long remoteCallTimeout, TimeUnit unit) {
      remoteCallTimeout(unit.toMillis(remoteCallTimeout));
      return this;
   }

   @Override
   public ClusterLoaderConfigurationBuilder withProperties(Properties p) {
      attributes.attribute(PROPERTIES).set(TypedProperties.toTypedProperties(p));
      XmlConfigHelper.setAttributes(attributes, p, false, true);
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public ClusterLoaderConfiguration create() {
      return new ClusterLoaderConfiguration(attributes.protect(), async.create(), singletonStore.create());
   }

   @Override
   public ClusterLoaderConfigurationBuilder read(ClusterLoaderConfiguration template) {
      super.read(template);
      return this;
   }
}
