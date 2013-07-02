package org.infinispan.query.backend;

import java.util.Properties;

import org.hibernate.search.spi.BuildContext;
import org.hibernate.search.spi.ServiceProvider;
import org.infinispan.factories.ComponentRegistry;

/**
 * Simple wrapper to make the Cache ComponentRegistry available to the services managed by
 * Hibernate Search
 * 
 * @author Sanne Grinovero
 * @since 5.2
 */
public final class ComponentRegistryServiceProvider implements ServiceProvider<ComponentRegistry> {

   private final ComponentRegistry cr;

   public ComponentRegistryServiceProvider(ComponentRegistry cr) {
      this.cr = cr;
   }

   @Override
   public void start(Properties properties, BuildContext context) {
      // no-op
   }

   @Override
   public ComponentRegistry getService() {
      return cr;
   }

   @Override
   public void stop() {
      // no-op
   }

}
