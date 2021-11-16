package org.infinispan.scripting.impl;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
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
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.scripting.ScriptingManager;
import org.infinispan.scripting.logging.Log;
import org.infinispan.scripting.utils.ScriptConversions;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.impl.Authorizer;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskManager;
import org.infinispan.util.concurrent.BlockingManager;
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

   @Inject EmbeddedCacheManager cacheManager;
   @Inject TaskManager taskManager;
   @Inject
   Authorizer authorizer;
   @Inject EncoderRegistry encoderRegistry;
   @Inject GlobalConfiguration globalConfiguration;
   @Inject BlockingManager blockingManager;

   private ScriptEngineManager scriptEngineManager;
   private ConcurrentMap<String, ScriptEngine> scriptEnginesByExtension = new ConcurrentHashMap<>(2);
   private ConcurrentMap<String, ScriptEngine> scriptEnginesByLanguage = new ConcurrentHashMap<>(2);
   private Cache<String, String> scriptCache;
   private ScriptConversions scriptConversions;
   ConcurrentMap<String, CompiledScript> compiledScripts = new ConcurrentHashMap<>();

   private final Function<String, ScriptEngine> getEngineByName = this::getEngineByName;
   private final Function<String, ScriptEngine> getEngineByExtension = this::getEngineByExtension;

   public ScriptingManagerImpl() {
   }

   @Start
   public void start() {
      ClassLoader classLoader = globalConfiguration.classLoader();
      this.scriptEngineManager = new ScriptEngineManager(classLoader);
      taskManager.registerTaskEngine(new ScriptingTaskEngine(this));
      scriptConversions = new ScriptConversions(encoderRegistry);
   }

   Cache<String, String> getScriptCache() {
      if (scriptCache == null && cacheManager != null) {
         scriptCache = SecurityActions.getCache(cacheManager, SCRIPT_CACHE);
      }
      return scriptCache;
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

   CompletionStage<Boolean> containsScriptAsync(String taskName) {
      return SecurityActions.getUnwrappedCache(getScriptCache()).getAsync(taskName)
            .thenApply(Objects::nonNull);
   }

   @Override
   public <T> CompletionStage<T> runScript(String scriptName) {
      return runScript(scriptName, new TaskContext());
   }


   @Override
   public <T> CompletionStage<T> runScript(String scriptName, TaskContext context) {
      ScriptMetadata metadata = getScriptMetadata(scriptName);
      if (authorizer != null) {
         AuthorizationManager authorizationManager = context.getCache().isPresent() ?
               SecurityActions.getAuthorizationManager(context.getCache().get().getAdvancedCache()) : null;
         if (authorizationManager != null) {
            // when the cache is secured
            authorizationManager.checkPermission(AuthorizationPermission.EXEC, metadata.role().orElse(null));
         } else {
            if (context.getSubject().isPresent()) {
               authorizer.checkPermission(context.getSubject().get(), AuthorizationPermission.EXEC);
            } else {
               authorizer.checkPermission(AuthorizationPermission.EXEC, metadata.role().orElse(null));
            }
         }
      }

      MediaType scriptMediaType = metadata.dataType();
      MediaType requestMediaType = context.getCache().map(c -> c.getAdvancedCache().getValueDataConversion().getRequestMediaType()).orElse(MediaType.MATCH_ALL);
      Bindings userBindings = context.getParameters()
            .map(p -> {
               Map<String, ?> params = scriptConversions.convertParameters(context);
               return new SimpleBindings((Map<String, Object>) params);
            })
            .orElse(new SimpleBindings());

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

   <T> CompletionStage<T> execute(ScriptMetadata metadata, Bindings bindings) {
      return blockingManager.supplyBlocking(() -> {
         CompiledScript compiled = compiledScripts.get(metadata.name());
         try {
            if (compiled != null) {
               return (T) compiled.eval(bindings);
            } else {
               ScriptEngine engine = getEngineForScript(metadata);
               String script = getScriptCache().get(metadata.name());
               return (T) engine.eval(script, bindings);
            }
         } catch (ScriptException e) {
            throw log.scriptExecutionError(e);
         }
      }, "ScriptingManagerImpl - execute");
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
