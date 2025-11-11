package org.infinispan.scripting.impl;

import static org.infinispan.commons.internal.InternalCacheNames.SCRIPT_CACHE_NAME;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.roastedroot.quickjs4j.core.Engine;
import io.roastedroot.quickjs4j.core.Runner;
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
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.security.impl.Authorizer;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.manager.TaskManager;
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
   private static final ThreadLocal<Integer> RUNNING_IN_SCRIPT = ThreadLocal.withInitial(() -> 0);

   @Inject
   EmbeddedCacheManager cacheManager;
   @Inject
   TaskManager taskManager;
   @Inject
   Authorizer authorizer;
   @Inject
   EncoderRegistry encoderRegistry;
   @Inject
   GlobalConfiguration globalConfiguration;
   @Inject
   BlockingManager blockingManager;
   // TODO: this should be injected I think
   // @Inject
   ObjectMapper objectMapper = new ObjectMapper();

   private Runner runner; // TODO: just a static one?
   private Cache<String, String> scriptCache;
   private ScriptConversions scriptConversions;

   public ScriptingManagerImpl() {
   }

   @Start
   public void start() {
      ClassLoader classLoader = globalConfiguration.classLoader();
      ScriptingJavaApi javaApi = new ScriptingJavaApi(cacheManager);
      Engine engine = Engine.builder()
              .addBuiltins(ScriptingJavaApi_Builtins.toBuiltins(javaApi))
              .addInvokables(JsApi_Invokables.toInvokables())
              .build();

      this.runner = Runner.builder()
              .withEngine(engine)
              .build();

      taskManager.registerTaskEngine(new ScriptingTaskEngine(this));
      scriptConversions = new ScriptConversions(encoderRegistry);
   }

   Cache<String, String> getScriptCache() {
      if (scriptCache == null && cacheManager != null) {
         scriptCache = SecurityActions.getCache(cacheManager, SCRIPT_CACHE_NAME);
      }
      return scriptCache;
   }

   @Override
   public void addScript(String name, String script) {
      addScript(name, script, ScriptMetadataParser.parse(name, script).build());
   }

   @Override
   public void addScript(String name, String script, ScriptMetadata metadata) {
      Objects.requireNonNull(name, "name");
      Objects.requireNonNull(script, "script");
      Objects.requireNonNull(metadata, "metadata");
      getScriptCache().getAdvancedCache().put(name, script, metadata);
   }

   @Override
   public void removeScript(String name) {
      Objects.requireNonNull(name, "name");
      if (containsScript(name)) {
         getScriptCache().remove(name);
      } else {
         throw log.noNamedScript(name);
      }
   }

   @Override
   public String getScript(String name) {
      Objects.requireNonNull(name, "name");
      if (containsScript(name)) {
         return getUnwrappedScriptCache().get(name);
      } else {
         throw log.noNamedScript(name);
      }
   }

   @Override
   public ScriptWithMetadata getScriptWithMetadata(String name) {
      Objects.requireNonNull(name, "name");
      CacheEntry<String, String> entry = getUnwrappedScriptCache().getAdvancedCache().getCacheEntry(name);
      if (entry != null) {
         return new ScriptWithMetadata(entry.getValue(), (ScriptMetadata) entry.getMetadata());
      } else {
         throw log.noNamedScript(name);
      }
   }

   @Override
   public Set<String> getScriptNames() {
      return getUnwrappedScriptCache().keySet();
   }

   @Override
   public boolean containsScript(String name) {
      Objects.requireNonNull(name, "name");
      return getUnwrappedScriptCache().containsKey(name);
   }

   CompletionStage<Boolean> containsScriptAsync(String name) {
      return getUnwrappedScriptCache().getAsync(name)
            .thenApply(Objects::nonNull);
   }

   private Cache<String, String> getUnwrappedScriptCache() {
      return SecurityActions.getUnwrappedCache(getScriptCache());
   }

   @Override
   public <T> CompletionStage<T> runScript(String name) {
      Objects.requireNonNull(name, "name");
      return runScript(name, new TaskContext());
   }

   @Override
   public <T> CompletionStage<T> runScript(String name, TaskContext context) {
      Objects.requireNonNull(name, "name");
      Objects.requireNonNull(context, "context");
      ScriptMetadata metadata = getScriptMetadata(name);
      if (authorizer != null) {
         AuthorizationManager authorizationManager = context.getCache().isPresent() ?
               SecurityActions.getCacheAuthorizationManager(context.getCache().get().getAdvancedCache()) : null;
         if (authorizationManager != null && !authorizationManager.isPermissive()) {
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

// TODO: this functionality should be implemented as Builtins!
//
//      Bindings userBindings = context.getParameters()
//            .map(p -> {
//               Map<String, ?> params = scriptConversions.convertParameters(context);
//               return new SimpleBindings((Map<String, Object>) params);
//            })
//            .orElse(new SimpleBindings());
//
//      SimpleBindings systemBindings = new SimpleBindings();
//      DataTypedCacheManager dataTypedCacheManager = new DataTypedCacheManager(scriptMediaType, cacheManager, context.getSubject().orElse(null));
//      systemBindings.put(SystemBindings.CACHE_MANAGER.toString(), dataTypedCacheManager);
//      systemBindings.put(SystemBindings.SCRIPTING_MANAGER.toString(), this);
//      context.getCache().ifPresent(cache -> {
//         if (!requestMediaType.equals(MediaType.MATCH_ALL)) {
//            cache = cache.getAdvancedCache().withMediaType(scriptMediaType, scriptMediaType);
//         }
//         systemBindings.put(SystemBindings.CACHE.toString(), cache);
//      });
//
//      context.getMarshaller().ifPresent(marshaller -> {
//         systemBindings.put(SystemBindings.MARSHALLER.toString(), marshaller);
//      });
//
      // TODO populate systemBindings
      JsonNode systemBindings = objectMapper.createObjectNode();

      // TODO: improve me!
      ObjectNode userBindings = objectMapper.createObjectNode();
      context.getParameters()
              .ifPresent(p -> {
               Map<String, ?> params = scriptConversions.convertParameters(context);
               params.entrySet().forEach(param ->
                       userBindings.put(param.getKey(), objectMapper.valueToTree(param.getValue())));
            });

      CacheScriptBindings bindings = new CacheScriptBindings(
              systemBindings,
              userBindings,
              context.getCache().orElse(null));

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

   <T> CompletionStage<T> execute(ScriptMetadata metadata, JsonNode userInput) {
      if (RUNNING_IN_SCRIPT.get() > 0) {
         return CompletableFuture.completedFuture(executeDirectly(metadata, userInput));
      }
      return blockingManager.supplyBlocking(() -> executeDirectly(metadata, userInput), "ScriptingManagerImpl - execute");
   }

   private <T> T executeDirectly(ScriptMetadata metadata, JsonNode userInput) {
      var initial = RUNNING_IN_SCRIPT.get();
      RUNNING_IN_SCRIPT.set(initial + 1);
      // CompiledScript compiled = compiledScripts.get(metadata.name());
      try {
         String script = getScriptCache().get(metadata.name());

         ScriptingJavaApi.JsApi jsApi = JsApi_Invokables.create(script, runner);

         var result = jsApi.process(userInput);
         return (T) result;
//         if (compiled != null) {
//            return (T) compiled.eval(bindings);
//         } else {
//            ScriptEngine engine = getEngineForScript(metadata);
//            String script = getScriptCache().get(metadata.name());
//            return (T) engine.eval(script, bindings);
//         }
//      } catch (ScriptException e) {
//         throw log.scriptExecutionError(e);
      } finally {
         RUNNING_IN_SCRIPT.set(initial);
      }
   }

}
