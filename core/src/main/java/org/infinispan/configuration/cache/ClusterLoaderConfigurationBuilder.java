package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.ClusterLoaderConfiguration.REMOTE_CALL_TIMEOUT;

import java.util.concurrent.TimeUnit;

/**
 * @deprecated since 11.0. To be removed in 14.0 ISPN-11864 with no direct replacement.
 */
@Deprecated
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
   public ClusterLoaderConfiguration create() {
      return new ClusterLoaderConfiguration(attributes.protect(), async.create());
   }

   @Override
   public ClusterLoaderConfigurationBuilder read(ClusterLoaderConfiguration template) {
      super.read(template);
      return this;
   }
}
