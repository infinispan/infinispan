package org.infinispan.scripting.impl;

import java.util.EnumSet;
import java.util.concurrent.ConcurrentMap;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.concurrent.NoOpFuture;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.CacheMgmtInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.scripting.ScriptingManager;
import org.infinispan.scripting.impl.ScriptMetadata.MetadataProperties;
import org.infinispan.scripting.logging.Log;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.impl.CacheRoleImpl;
import org.infinispan.util.logging.LogFactory;

/**
 * ScriptingManagerImpl.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
@Scope(Scopes.GLOBAL)
public class ScriptingManagerImpl implements ScriptingManager {
   public static final String SCRIPT_MANAGER_ROLE = "___script_manager";
   public static final String SCRIPT_CACHE = "___script_cache";
   private static final String DEFAULT_SCRIPT_EXTENSION = "js";
   private static final Log log = LogFactory.getLog(ScriptingManagerImpl.class, Log.class);
   EmbeddedCacheManager cacheManager;
   private ScriptEngineManager scriptEngineManager;
   private ConcurrentMap<String, ScriptEngine> scriptEnginesByExtension = CollectionFactory.makeConcurrentMap(2);
   private ConcurrentMap<String, ScriptEngine> scriptEnginesByLanguage = CollectionFactory.makeConcurrentMap(2);
   Cache<String, String> scriptCache;
   ConcurrentMap<String, CompiledScript> compiledScripts = CollectionFactory.makeConcurrentMap();
   private AuthorizationManager authzManager;
   private Marshaller marshaller;


   public ScriptingManagerImpl() {
   }

   @Inject
   public void initialize(final EmbeddedCacheManager cacheManager, InternalCacheRegistry internalCacheRegistry) {
      this.cacheManager = cacheManager;
      ClassLoader classLoader = cacheManager.getCacheManagerConfiguration().classLoader();
      this.scriptEngineManager = new ScriptEngineManager(classLoader);
      internalCacheRegistry.registerInternalCache(SCRIPT_CACHE, getScriptCacheConfiguration().build(), EnumSet.of(InternalCacheRegistry.Flag.USER));
   }

   Cache<String, String> getScriptCache() {
      if (scriptCache == null) {
         scriptCache = cacheManager.getCache(SCRIPT_CACHE);
      }
      return scriptCache;
   }

   private ConfigurationBuilder getScriptCacheConfiguration() {
      GlobalConfiguration globalConfiguration = cacheManager.getGlobalComponentRegistry().getGlobalConfiguration();
      CacheMode cacheMode = globalConfiguration.isClustered() ? CacheMode.REPL_SYNC : CacheMode.LOCAL;

      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.clustering().cacheMode(cacheMode).sync().stateTransfer().fetchInMemoryState(true).awaitInitialTransfer(false).compatibility().enable()
            .marshaller(new GenericJBossMarshaller()).customInterceptors().addInterceptor().interceptor(new ScriptingInterceptor()).before(CacheMgmtInterceptor.class);
      if (globalConfiguration.security().authorization().enabled()) {
         globalConfiguration.security().authorization().roles().put(SCRIPT_MANAGER_ROLE, new CacheRoleImpl(SCRIPT_MANAGER_ROLE, AuthorizationPermission.ALL));
         cfg.security().authorization().enable().role(SCRIPT_MANAGER_ROLE);
      }
      return cfg;
   }

   ScriptMetadata compileScript(String name, String script) {
      ScriptMetadata metadata = extractMetadataFromScript(name, script);
      ScriptEngine engine = getEngineForScript(metadata);
      if (engine instanceof Compilable) {
         try {
            CompiledScript compiledScript = ((Compilable) engine).compile(script);
            compiledScripts.put(name, compiledScript);
            return metadata;
         } catch (ScriptException e) {
            throw log.scriptCompilationException(e, name);
         }
      } else {
         return null;
      }
   }

   @Override
   public void addScript(String name, String script) {
      ScriptMetadata metadata = extractMetadataFromScript(name, script);
      ScriptEngine engine = getEngineForScript(metadata);
      if (engine == null) {
         throw log.noScriptEngineForScript(name);
      }
      getScriptCache().getAdvancedCache().put(name, script, metadata);
   }

   @Override
   public void removeScript(String name) {
      if (getScriptCache().remove(name) == null) {
         throw log.noNamedScript(name);
      }
   }

   @Override
   public void setMarshaller(Marshaller marshaller) {
      this.marshaller = marshaller;
   }

   @Override
   public Marshaller getMarshaller() {
         return this.marshaller;
   }

   @Override
   public <T> NotifyingFuture<T> runScript(String scriptName, Bindings parameters) {
      return runScript(scriptName, null, parameters);
   }

   @Override
   public <T> NotifyingFuture<T> runScript(String scriptName) {
      return runScript(scriptName, null, new SimpleBindings());
   }

   @Override
   public <T> NotifyingFuture<T> runScript(String scriptName, Cache<?, ?> cache) {
      return runScript(scriptName, cache, new SimpleBindings());
   }

   @Override
   public <T> NotifyingFuture<T> runScript(String scriptName, Cache<?, ?> cache, Bindings parameters) {
      if (authzManager != null) {
         authzManager.checkPermission(AuthorizationPermission.EXEC);
      }
      ScriptMetadata metadata = getScriptMetadata(scriptName);

      SimpleBindings systemBindings = new SimpleBindings();
      systemBindings.put(SystemBindings.CACHE_MANAGER.toString(), cacheManager);
      systemBindings.put(SystemBindings.SCRIPTING_MANAGER.toString(), this);
      if (cache != null) {
         systemBindings.put(SystemBindings.CACHE.toString(), cache);
      }
      CacheScriptBindings bindings = new CacheScriptBindings(systemBindings, parameters);

      String mode = metadata.property(MetadataProperties.MODE);
      ScriptRunner runner = ExecutionMode.valueOf(mode.toUpperCase()).getRunner();

      return runner.runScript(this, metadata, bindings);
   }

   ScriptMetadata getScriptMetadata(String scriptName) {
      CacheEntry<String, String> scriptEntry = SecurityActions.getCacheEntry(getScriptCache().getAdvancedCache(), scriptName);
      if (scriptEntry == null) {
         throw log.noNamedScript(scriptName);
      }
      ScriptMetadata metadata = (ScriptMetadata) scriptEntry.getMetadata();
      return metadata;
   }

   <T> NotifyingFuture<T> execute(ScriptMetadata metadata, Bindings bindings) {
      CompiledScript compiled = compiledScripts.get(metadata.name());
      try {
         if (compiled != null) {
            T result = (T) compiled.eval(bindings);
            return new NoOpFuture<T>(result);
         } else {
            ScriptEngine engine = getEngineForScript(metadata);
            T result = (T) engine.eval(getScriptCache().get(metadata.name()), bindings);
            return new NoOpFuture<T>(result);
         }
      } catch (ScriptException e) {
         throw log.scriptExecutionError(e);
      }
   }

   private ScriptMetadata extractMetadataFromScript(String name, String script) {
      ScriptMetadata.Builder metadataBuilder = new ScriptMetadata.Builder();

      metadataBuilder.property(MetadataProperties.NAME, name);
      metadataBuilder.property(MetadataProperties.MODE, ExecutionMode.LOCAL.toString().toLowerCase());
      int s = name.lastIndexOf(".") + 1;
      if (s == 0 || s == name.length())
         metadataBuilder.property(MetadataProperties.EXTENSION, DEFAULT_SCRIPT_EXTENSION);
      else
         metadataBuilder.property(MetadataProperties.EXTENSION, name.substring(s));
      if (script.startsWith("//")) {
         int state = KEY;
         String key = null;
         String value = null;
         StringBuilder sb = new StringBuilder();
         char ch = 0;
         for (int pos = 2; ch != 10 && ch != 13 && pos < script.length(); pos++) {
            ch = script.charAt(pos);
            switch (state) {
            case KEY:
               switch (ch) {
               case '=':
                  key = sb.toString().toUpperCase();
                  sb = new StringBuilder();
                  state = VALUE;
                  break;
               case ' ':
                  break;
               default:
                  sb.append(ch);
               }
               break;
            case VALUE:
               switch (ch) {
               case ',':
               case 10:
               case 13:
                  value = sb.toString();
                  metadataBuilder.property(MetadataProperties.valueOf(key), value);
                  sb = new StringBuilder();
                  state = KEY;
                  break;
               case ' ':
                  break;
               default:
                  sb.append(ch);
               }
               break;
            }
         }
      }

      return metadataBuilder.build();
   }

   ScriptEngine getEngineForScript(ScriptMetadata metadata) {
      String language = metadata.property(MetadataProperties.LANGUAGE);
      if (language != null) {
         if (scriptEnginesByLanguage.containsKey(language)) {
            return scriptEnginesByLanguage.get(language);
         } else {
            ScriptEngine engine = scriptEngineManager.getEngineByName(language);
            if (engine == null) {
               throw log.noEngineForScript(metadata.name());
            } else {
               scriptEnginesByLanguage.put(language, engine);
               return engine;
            }
         }
      } else {
         String extension = metadata.property(MetadataProperties.EXTENSION);
         if (scriptEnginesByExtension.containsKey(extension)) {
            return scriptEnginesByExtension.get(extension);
         } else {
            ScriptEngine engine = scriptEngineManager.getEngineByExtension(extension);
            if (engine == null) {
               throw log.noEngineForScript(metadata.name());
            } else {
               scriptEnginesByExtension.put(extension, engine);
               return engine;
            }
         }
      }

   }

   private static final int KEY = 0;
   private static final int VALUE = 1;

}
