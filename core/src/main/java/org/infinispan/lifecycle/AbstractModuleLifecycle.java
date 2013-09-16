package org.infinispan.lifecycle;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;


/**
 * A default, no-op implementation of the {@link org.infinispan.lifecycle.ModuleLifecycle} interface, designed for easy
 * extension.
 *
 * @author Manik Surtani
 * @version 4.0
 */
public class AbstractModuleLifecycle implements ModuleLifecycle {

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      // a no-op
   }

   @Override
   public void cacheManagerStarted(GlobalComponentRegistry gcr) {
      // a no-op
   }

   @Override
   public void cacheManagerStopping(GlobalComponentRegistry gcr) {
      // a no-op
   }

   @Override
   public void cacheManagerStopped(GlobalComponentRegistry gcr) {
      // a no-op
   }

   @Override
   public void cacheStarting(ComponentRegistry cr, Configuration configuration, String cacheName) {
      // a no-op
   }

   @Override
   public void cacheStarted(ComponentRegistry cr, String cacheName) {
      // a no-op
   }

   @Override
   public void cacheStopping(ComponentRegistry cr, String cacheName) {
      // a no-op
   }

   @Override
   public void cacheStopped(ComponentRegistry cr, String cacheName) {
      // a no-op
   }
}
