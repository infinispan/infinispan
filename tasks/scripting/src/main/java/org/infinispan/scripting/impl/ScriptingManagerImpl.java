package org.infinispan.scripting.impl;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Function;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.impl.CacheMgmtInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.registry.InternalCacheRegistry.Flag;
import org.infinispan.scripting.ScriptingManager;
import org.infinispan.scripting.logging.Log;
import org.infinispan.scripting.utils.ScriptConversions;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.impl.AuthorizationHelper;
import org.infinispan.security.impl.CacheRoleImpl;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskManager;
import org.infinispan.util.logging.LogFactory;


/**
 * ScriptingManagerImpl.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
@Scope(Scopes.GLOBAL)
public class ScriptingManagerImpl implements ScriptingManager {
   private static final Log log = LogFactory.getLog(ScriptingManagerImpl.class, Log.class);

   @Inject
   private EmbeddedCacheManager cacheManager;
   @Inject
   private TaskManager taskManager;
   @Inject
   private InternalCacheRegistry internalCacheRegistry;
   @Inject
   private EncoderRegistry encoderRegistry;

   private ScriptEngineManager scriptEngineManager;
   private ConcurrentMap<String, ScriptEngine> scriptEnginesByExtension = CollectionFactory.makeConcurrentMap(2);
   private ConcurrentMap<String, ScriptEngine> scriptEnginesByLanguage = CollectionFactory.makeConcurrentMap(2);
   private Cache<String, String> scriptCache;
   private ScriptConversions scriptConversions;
   ConcurrentMap<String, CompiledScript> compiledScripts = CollectionFactory.makeConcurrentMap();
   private AuthorizationHelper globalAuthzHelper;

   private final Function<String, ScriptEngine> getEngineByName = this::getEngineByName;
   private final Function<String, ScriptEngine> getEngineByExtension = this::getEngineByExtension;

   public ScriptingManagerImpl() {
   }

   @Start
   public void start() {
      ClassLoader classLoader = cacheManager.getCacheManagerConfiguration().classLoader();
      this.scriptEngineManager = new ScriptEngineManager(classLoader);
      internalCacheRegistry.registerInternalCache(SCRIPT_CACHE, getScriptCacheConfiguration().build(), EnumSet.of(Flag.USER, Flag.PROTECTED, Flag.PERSISTENT, Flag.GLOBAL));
      taskManager.registerTaskEngine(new ScriptingTaskEngine(this));
      scriptConversions = new ScriptConversions(encoderRegistry);
   }

   Cache<String, String> getScriptCache() {
      if (scriptCache == null) {
         scriptCache = cacheManager.getCache(SCRIPT_CACHE);
      }
      return scriptCache;
   }

   private ConfigurationBuilder getScriptCacheConfiguration() {
      GlobalConfiguration globalConfiguration = cacheManager.getGlobalComponentRegistry().getGlobalConfiguration();

      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.encoding().key().mediaType(APPLICATION_OBJECT_TYPE);
      cfg.encoding().value().mediaType(APPLICATION_OBJECT_TYPE);
      cfg.customInterceptors().addInterceptor().interceptor(new ScriptingInterceptor()).before(CacheMgmtInterceptor.class);
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
         return SecurityActions.getUnwrappedCache(getScriptCache()).get(name);
      } else {
         throw log.noNamedScript(name);
      }
   }

   @Override
   public Set<String> getScriptNames() {
      return SecurityActions.getUnwrappedCache(getScriptCache()).keySet();
   }

   boolean containsScript(String taskName) {
      return SecurityActions.getUnwrappedCache(getScriptCache()).containsKey(taskName);
   }

   @Override
   public <T> CompletableFuture<T> runScript(String scriptName) {
      return runScript(scriptName, new TaskContext());
   }


   @Override
   public <T> CompletableFuture<T> runScript(String scriptName, TaskContext context) {
      ScriptMetadata metadata = getScriptMetadata(scriptName);
      if (globalAuthzHelper != null) {
         AuthorizationManager authorizationManager = context.getCache().isPresent() ?
               SecurityActions.getAuthorizationManager(context.getCache().get().getAdvancedCache()) : null;
         if (authorizationManager != null) {
            authorizationManager.checkPermission(AuthorizationPermission.EXEC, metadata.role().orElse(null));
         } else {
            globalAuthzHelper.checkPermission(AuthorizationPermission.EXEC, metadata.role().orElse(null));
         }

      }

      String scriptMediaType = metadata.dataType().toString();
      MediaType requestMediaType = context.getCache().map(c -> c.getAdvancedCache().getValueDataConversion().getRequestMediaType()).orElse(MediaType.MATCH_ALL);
      Bindings userBindings = context.getParameters()
            .map(p -> {
               Map<String, ?> params = scriptConversions.convertParameters(context);
               return new SimpleBindings((Map<String, Object>) params);
            })
            .orElseGet(() -> new SimpleBindings());

      SimpleBindings systemBindings = new SimpleBindings();
      DataTypedCacheManager dataTypedCacheManager = new DataTypedCacheManager(scriptMediaType, cacheManager, context.getSubject().orElse(null));
      systemBindings.put(SystemBindings.CACHE_MANAGER.toString(), dataTypedCacheManager);
      systemBindings.put(SystemBindings.SCRIPTING_MANAGER.toString(), this);
      context.getCache().ifPresent(cache -> {
         if (requestMediaType != null && !requestMediaType.equals(MediaType.MATCH_ALL)) {
            cache = cache.getAdvancedCache().withMediaType(scriptMediaType, scriptMediaType);
         }
         systemBindings.put(SystemBindings.CACHE.toString(), cache);
      });

      context.getMarshaller().ifPresent(marshaller -> {
         systemBindings.put(SystemBindings.MARSHALLER.toString(), marshaller);
      });

      CacheScriptBindings bindings = new CacheScriptBindings(systemBindings, userBindings);

      ScriptRunner runner = metadata.mode().getRunner();

      return runner.runScript(this, metadata, bindings).thenApply(t ->
            (T) scriptConversions.convertToRequestType(t, metadata.dataType(), requestMediaType));
   }

   ScriptMetadata getScriptMetadata(String scriptName) {
      CacheEntry<String, String> scriptEntry = SecurityActions.getCacheEntry(getScriptCache().getAdvancedCache(), scriptName);
      if (scriptEntry == null) {
         throw log.noNamedScript(scriptName);
      }
      return (ScriptMetadata) scriptEntry.getMetadata();
   }

   <T> CompletableFuture<T> execute(ScriptMetadata metadata, Bindings bindings) {
      CompiledScript compiled = compiledScripts.get(metadata.name());
      try {
         if (compiled != null) {
            T result = (T) compiled.eval(bindings);
            return CompletableFuture.completedFuture(result);
         } else {
            ScriptEngine engine = getEngineForScript(metadata);
            String script = getScriptCache().get(metadata.name());
            T result = (T) engine.eval(script, bindings);
            return CompletableFuture.completedFuture(result);
         }
      } catch (ScriptException e) {
         throw log.scriptExecutionError(e);
      }
   }

   ScriptEngine getEngineForScript(ScriptMetadata metadata) {
      ScriptEngine engine;
      if (metadata.language().isPresent()) {
         engine = scriptEnginesByLanguage.computeIfAbsent(metadata.language().get(), getEngineByName);
      } else {
         engine = scriptEnginesByExtension.computeIfAbsent(metadata.extension(), getEngineByExtension);
      }
      if (engine == null) {
         throw log.noEngineForScript(metadata.name());
      } else {
         return engine;
      }
   }

   private ScriptEngine getEngineByName(String shortName) {
      return withClassLoader(ScriptingManagerImpl.class.getClassLoader(),
            scriptEngineManager, shortName,
            ScriptEngineManager::getEngineByName);
   }

   private ScriptEngine getEngineByExtension(String extension) {
      return withClassLoader(ScriptingManagerImpl.class.getClassLoader(),
            scriptEngineManager, extension,
            ScriptEngineManager::getEngineByExtension);
   }

   private static ScriptEngine withClassLoader(ClassLoader cl,
                                               ScriptEngineManager manager, String name,
                                               BiFunction<ScriptEngineManager, String, ScriptEngine> f) {
      ClassLoader curr = Thread.currentThread().getContextClassLoader();
      try {
         Thread.currentThread().setContextClassLoader(cl);
         return f.apply(manager, name);
      } finally {
         Thread.currentThread().setContextClassLoader(curr);
      }
   }

}
