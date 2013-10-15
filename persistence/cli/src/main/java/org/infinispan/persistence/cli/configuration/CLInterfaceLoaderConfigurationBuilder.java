package org.infinispan.persistence.cli.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
public class CLInterfaceLoaderConfigurationBuilder
      extends AbstractStoreConfigurationBuilder
                    <CLInterfaceLoaderConfiguration, CLInterfaceLoaderConfigurationBuilder> {

   private String connectionString;

   public CLInterfaceLoaderConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder);
   }

   public CLInterfaceLoaderConfigurationBuilder connectionString(String connectionString) {
      this.connectionString = connectionString;
      return this;
   }

   @Override
   public CLInterfaceLoaderConfiguration create() {
      return new CLInterfaceLoaderConfiguration(purgeOnStartup, fetchPersistentState,
            ignoreModifications, async.create(), singletonStore.create(),
            preload, shared, properties, connectionString);
   }

   @Override
   public Builder<?> read(CLInterfaceLoaderConfiguration template) {
      this.connectionString = template.connectionString();
      return this;
   }

   @Override
   public CLInterfaceLoaderConfigurationBuilder self() {
      return this;
   }

}
