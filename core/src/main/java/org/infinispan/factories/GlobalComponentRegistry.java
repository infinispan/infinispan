package org.infinispan.factories;

import org.infinispan.CacheException;
import org.infinispan.config.GlobalConfiguration;
import static org.infinispan.config.GlobalConfiguration.ShutdownHookBehavior.DEFAULT;
import static org.infinispan.config.GlobalConfiguration.ShutdownHookBehavior.REGISTER;
import org.infinispan.factories.annotations.NonVolatile;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.CacheManagerJmxRegistration;
import org.infinispan.logging.Log;
import org.infinispan.logging.LogFactory;
import org.infinispan.manager.CacheManager;

import javax.management.MBeanServerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * A global component registry where shared components are stored.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
@NonVolatile
public class GlobalComponentRegistry extends AbstractComponentRegistry {

   private Log log = LogFactory.getLog(GlobalComponentRegistry.class);
   private static final String NAMED_REGISTRY_PREFIX = "NamedComponentRegistry:";
   /**
    * Hook to shut down the cache when the JVM exits.
    */
   private Thread shutdownHook;
   /**
    * A flag that the shutdown hook sets before calling cache.stop().  Allows stop() to identify if it has been called
    * from a shutdown hook.
    */
   private boolean invokedFromShutdownHook;

   private GlobalConfiguration globalConfiguration;

   /**
    * Creates an instance of the component registry.  The configuration passed in is automatically registered.
    *
    * @param configuration configuration with which this is created
    */
   public GlobalComponentRegistry(GlobalConfiguration configuration, CacheManager cacheManager) {
      if (configuration == null) throw new NullPointerException("GlobalConfiguration cannot be null!");
      try {
         // this order is important ... 
         globalConfiguration = configuration;
         registerDefaultClassLoader(null);
         registerComponent(this, GlobalComponentRegistry.class);
         registerComponent(cacheManager, CacheManager.class);
         registerComponent(configuration, GlobalConfiguration.class);
         registerComponent(new CacheManagerJmxRegistration(), CacheManagerJmxRegistration.class);
      }
      catch (Exception e) {
         throw new CacheException("Unable to construct a GlobalComponentRegistry!", e);
      }
   }

   protected Log getLog() {
      return log;
   }

   @Override
   protected void removeShutdownHook() {
      // if this is called from a source other than the shutdown hook, deregister the shutdown hook.
      if (!invokedFromShutdownHook && shutdownHook != null) Runtime.getRuntime().removeShutdownHook(shutdownHook);
   }

   @Override
   protected void addShutdownHook() {
      ArrayList al = MBeanServerFactory.findMBeanServer(null);
      boolean registerShutdownHook = (globalConfiguration.getShutdownHookBehavior() == DEFAULT && al.size() == 0)
            || globalConfiguration.getShutdownHookBehavior() == REGISTER;

      if (registerShutdownHook) {
         log.trace("Registering a shutdown hook.  Configured behavior = {0}", globalConfiguration.getShutdownHookBehavior());
         shutdownHook = new Thread() {
            @Override
            public void run() {
               try {
                  invokedFromShutdownHook = true;
                  GlobalComponentRegistry.this.stop();
               }
               finally {
                  invokedFromShutdownHook = false;
               }
            }
         };

         Runtime.getRuntime().addShutdownHook(shutdownHook);
      } else {

         log.trace("Not registering a shutdown hook.  Configured behavior = {0}", globalConfiguration.getShutdownHookBehavior());
      }
   }

   Map<String, ComponentRegistry> namedComponents = new HashMap<String, ComponentRegistry>();

   public final ComponentRegistry getNamedComponentRegistry(String name) {
      return namedComponents.get(name);
   }

   public final void registerNamedComponentRegistry(ComponentRegistry componentRegistry, String name) {
      namedComponents.put(name, componentRegistry);
   }

   public final void unregisterNamedComponentRegistry(String name) {
      namedComponents.remove(name);
   }

   public final void rewireNamedRegistries() {
      for (ComponentRegistry cr : namedComponents.values())
         cr.rewire();
   }
}
