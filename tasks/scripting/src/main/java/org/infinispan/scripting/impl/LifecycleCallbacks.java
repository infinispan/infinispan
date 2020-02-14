package org.infinispan.scripting.impl;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;
import static org.infinispan.scripting.ScriptingManager.SCRIPT_CACHE;
import static org.infinispan.scripting.ScriptingManager.SCRIPT_MANAGER_ROLE;

import java.util.EnumSet;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.impl.CacheMgmtInterceptor;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.scripting.ScriptingManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.impl.CacheRoleImpl;

/**
 * LifecycleCallbacks.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
@InfinispanModule(name = "scripting", requiredModules = "core")
public class LifecycleCallbacks implements ModuleLifecycle {

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration gc) {
      ScriptingManagerImpl scriptingManager = new ScriptingManagerImpl();
      gcr.registerComponent(scriptingManager, ScriptingManager.class);

      SerializationContextRegistry ctxRegistry = gcr.getComponent(SerializationContextRegistry.class);
      ctxRegistry.addContextInitializer(SerializationContextRegistry.MarshallerType.PERSISTENCE, new PersistenceContextInitializerImpl());

      BasicComponentRegistry bcr = gcr.getComponent(BasicComponentRegistry.class);
      InternalCacheRegistry internalCacheRegistry = bcr.getComponent(InternalCacheRegistry.class).wired();
      internalCacheRegistry.registerInternalCache(SCRIPT_CACHE, getScriptCacheConfiguration(gc).build(),
                                                  EnumSet.of(InternalCacheRegistry.Flag.USER,
                                                             InternalCacheRegistry.Flag.PROTECTED,
                                                             InternalCacheRegistry.Flag.PERSISTENT,
                                                             InternalCacheRegistry.Flag.GLOBAL));
   }

   @Override
   public void cacheManagerStarted(GlobalComponentRegistry gcr) {
      ScriptingManagerImpl scriptingManager = (ScriptingManagerImpl) gcr.getComponent(ScriptingManager.class);
      scriptingManager.getScriptCache();
   }

   @Override
   public void cacheStarting(ComponentRegistry cr, Configuration configuration, String cacheName) {
      if (SCRIPT_CACHE.equals(cacheName)) {
         BasicComponentRegistry bcr = cr.getComponent(BasicComponentRegistry.class);
         ScriptingInterceptor scriptingInterceptor = new ScriptingInterceptor();
         bcr.registerComponent(ScriptingInterceptor.class, scriptingInterceptor, true);
         bcr.addDynamicDependency(AsyncInterceptorChain.class.getName(), ScriptingInterceptor.class.getName());
         bcr.getComponent(AsyncInterceptorChain.class).wired()
            .addInterceptorAfter(scriptingInterceptor, CacheMgmtInterceptor.class);
      }
   }

   private ConfigurationBuilder getScriptCacheConfiguration(GlobalConfiguration globalConfiguration) {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.encoding().key().mediaType(APPLICATION_OBJECT_TYPE);
      cfg.encoding().value().mediaType(APPLICATION_OBJECT_TYPE);
      if (globalConfiguration.security().authorization().enabled()) {
         globalConfiguration.security().authorization().roles()
                            .put(SCRIPT_MANAGER_ROLE,
                                 new CacheRoleImpl(SCRIPT_MANAGER_ROLE, AuthorizationPermission.ALL));
         cfg.security().authorization().enable().role(SCRIPT_MANAGER_ROLE);
      }
      return cfg;
   }
}
