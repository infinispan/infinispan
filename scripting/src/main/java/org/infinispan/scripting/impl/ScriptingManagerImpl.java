   package org.infinispan.scripting.impl;

import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.commons.util.CollectionFactory;
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
import org.infinispan.scripting.logging.Log;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.impl.AuthorizationHelper;
import org.infinispan.security.impl.CacheRoleImpl;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskManager;
import org.infinispan.tasks.impl.TaskManagerImpl;
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
   private static final Log log = LogFactory.getLog(ScriptingManagerImpl.class, Log.class);
   EmbeddedCacheManager cacheManager;
   private ScriptEngineManager scriptEngineManager;
   private ConcurrentMap<String, ScriptEngine> scriptEnginesByExtension = CollectionFactory.makeConcurrentMap(2);
   private ConcurrentMap<String, ScriptEngine> scriptEnginesByLanguage = CollectionFactory.makeConcurrentMap(2);
   private Cache<String, String> scriptCache;
   ConcurrentMap<String, CompiledScript> compiledScripts = CollectionFactory.makeConcurrentMap();
   private AuthorizationHelper globalAuthzHelper;

   public ScriptingManagerImpl() {
   }

   @Inject
   public void initialize(final EmbeddedCacheManager cacheManager, InternalCacheRegistry internalCacheRegistry, TaskManager taskManager) {
      this.cacheManager = cacheManager;
      ClassLoader classLoader = cacheManager.getCacheManagerConfiguration().classLoader();
      this.scriptEngineManager = new ScriptEngineManager(classLoader);
      internalCacheRegistry.registerInternalCache(SCRIPT_CACHE, getScriptCacheConfiguration().build(), EnumSet.of(InternalCacheRegistry.Flag.USER, InternalCacheRegistry.Flag.PERSISTENT));
      ((TaskManagerImpl)taskManager).registerTaskEngine(new ScriptingTaskEngine(this));
   }

   Cache<String, String> getScriptCache() {
      if (scriptCache == null) {
         scriptCache = SecurityActions.getUnwrappedCache(cacheManager.getCache(SCRIPT_CACHE));
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
         globalAuthzHelper = cacheManager.getGlobalComponentRegistry().getComponent(AuthorizationHelper.class);
      }
      return cfg;
   }

   ScriptMetadata compileScript(String name, String script) {
      ScriptMetadata metadata = ScriptMetadataParser.parse(name, script);
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
      ScriptMetadata metadata = ScriptMetadataParser.parse(name, script);
      ScriptEngine engine = getEngineForScript(metadata);
      if (engine == null) {
         throw log.noScriptEngineForScript(name);
      }
      getScriptCache().getAdvancedCache().put(name, script, metadata);
   }

   @Override
   public void removeScript(String name) {
      if (containsScript(name)) {
         getScriptCache().remove(name);
      } else {
         throw log.noNamedScript(name);
      }
   }

   @Override
   public String getScript(String name) {
      if (containsScript(name)) {
         return getScriptCache().get(name);
      } else {
         throw log.noNamedScript(name);
      }
   }

   public boolean containsScript(String taskName) {
      return getScriptCache().containsKey(taskName);
   }

   @Override
   public <T> CompletableFuture<T> runScript(String scriptName) {
      return runScript(scriptName, new TaskContext());
   }

   @Override
   public <T> CompletableFuture<T> runScript(String scriptName, TaskContext context) {
      ScriptMetadata metadata = getScriptMetadata(scriptName);
      if (globalAuthzHelper != null) {
         if (context.getCache().isPresent()) {
            AuthorizationManager authorizationManager = SecurityActions.getAuthorizationManager(context.getCache().get().getAdvancedCache());
            authorizationManager.checkPermission(AuthorizationPermission.EXEC, metadata.role().orElse(null));
         } else {
            globalAuthzHelper.checkPermission(AuthorizationPermission.EXEC, metadata.role().orElse(null));
         }

      }

      Bindings userBindings = context.getParameters()
         .map(p -> {
            Map<String, ?> params = metadata.dataType().transformer.toDataType(context.getParameters().get(), context.getMarshaller());
            return new SimpleBindings((Map<String, Object>) params);
         })
         .orElseGet(() -> new SimpleBindings());

      SimpleBindings systemBindings = new SimpleBindings();
      systemBindings.put(SystemBindings.CACHE_MANAGER.toString(), cacheManager);
      systemBindings.put(SystemBindings.SCRIPTING_MANAGER.toString(), this);
      context.getCache().ifPresent(cache -> {
         systemBindings.put(SystemBindings.CACHE.toString(), cache);
      });

      context.getMarshaller().ifPresent(marshaller -> {
         systemBindings.put(SystemBindings.MARSHALLER.toString(), marshaller);
      });

      CacheScriptBindings bindings = new CacheScriptBindings(systemBindings, userBindings);

      ScriptRunner runner = metadata.mode().getRunner();

      return runner.runScript(this, metadata, bindings).thenApply(t ->
            (T) metadata.dataType().transformer.fromDataType(t, context.getMarshaller()));
   }

   ScriptMetadata getScriptMetadata(String scriptName) {
      CacheEntry<String, String> scriptEntry = SecurityActions.getCacheEntry(getScriptCache().getAdvancedCache(), scriptName);
      if (scriptEntry == null) {
         throw log.noNamedScript(scriptName);
      }
      ScriptMetadata metadata = (ScriptMetadata) scriptEntry.getMetadata();
      return metadata;
   }

   <T> CompletableFuture<T> execute(ScriptMetadata metadata, Bindings bindings) {
      CompiledScript compiled = compiledScripts.get(metadata.name());
      try {
         if (compiled != null) {
            T result = (T) compiled.eval(bindings);
            return CompletableFuture.completedFuture(result);
         } else {
            ScriptEngine engine = getEngineForScript(metadata);
            T result = (T) engine.eval(getScriptCache().get(metadata.name()), bindings);
            return CompletableFuture.completedFuture(result);
         }
      } catch (ScriptException e) {
         throw log.scriptExecutionError(e);
      }
   }

   ScriptEngine getEngineForScript(ScriptMetadata metadata) {
      ScriptEngine engine;
      if (metadata.language().isPresent()) {
         engine = scriptEnginesByLanguage.computeIfAbsent(metadata.language().get(),
               scriptEngineManager::getEngineByName);
      } else {
         engine = scriptEnginesByExtension.computeIfAbsent(metadata.extension(),
               scriptEngineManager::getEngineByExtension);
      }
      if (engine == null) {
         throw log.noEngineForScript(metadata.name());
      } else {
         return engine;
      }
   }
}
