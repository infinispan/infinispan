package org.infinispan.scripting.engines.jshell;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.script.Bindings;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

import jdk.jshell.DeclarationSnippet;
import jdk.jshell.Diag;
import jdk.jshell.EvalException;
import jdk.jshell.JShell;
import jdk.jshell.JShellException;
import jdk.jshell.Snippet;
import jdk.jshell.SnippetEvent;
import jdk.jshell.SourceCodeAnalysis;
import jdk.jshell.execution.DirectExecutionControl;
import jdk.jshell.execution.LocalExecutionControlProvider;
import jdk.jshell.spi.ExecutionControl;
import jdk.jshell.spi.ExecutionControlProvider;
import jdk.jshell.spi.ExecutionEnv;

/**
 * Compiled script of a {@link JShellScriptEngine}.
 * @author Eric Obermühlner
 */
public class JShellCompiledScript extends CompiledScript {
   private final JShellScriptEngine engine;
   private final List<String> snippets;

   JShellCompiledScript(JShellScriptEngine engine, String script) throws ScriptException {
      this.engine = engine;

      try (JShell jshell = JShell.builder()
            .executionEngine(new LocalExecutionControlProvider(), null)
            .build()) {
         this.snippets = compileScript(jshell, script);
      }
   }

   private static List<String> compileScript(JShell jshell, String script) throws ScriptException {
      List<String> snippets = new ArrayList<>();
      while (!script.isEmpty()) {
         SourceCodeAnalysis.CompletionInfo completionInfo = jshell.sourceCodeAnalysis().analyzeCompletion(script);
         if (!completionInfo.completeness().isComplete()) {
            throw new ScriptException("Incomplete script\n" + script);
         }
         snippets.add(completionInfo.source());
         script = completionInfo.remaining();
      }
      return snippets;
   }

   @Override
   public Object eval(ScriptContext context) throws ScriptException {
      Bindings globalBindings = context.getBindings(ScriptContext.GLOBAL_SCOPE);
      Bindings engineBindings = context.getBindings(ScriptContext.ENGINE_SCOPE);

      final AccessDirectExecutionControl accessDirectExecutionControl = new AccessDirectExecutionControl();
      try (JShell jshell = JShell.builder()
            .executionEngine(new AccessDirectExecutionControlProvider(accessDirectExecutionControl), null)
            .build()) {
         pushVariables(jshell, accessDirectExecutionControl, globalBindings, engineBindings);
         Object result = evaluateSnippets(jshell, accessDirectExecutionControl);
         pullVariables(jshell, accessDirectExecutionControl, globalBindings, engineBindings);

         return result;
      }
   }

   private void pushVariables(JShell jshell, AccessDirectExecutionControl accessDirectExecutionControl, Bindings globalBindings, Bindings engineBindings) throws ScriptException {
      Map<String, Object> variables = mergeBindings(globalBindings, engineBindings);
      VariablesTransfer.setVariables(variables);

      for (Map.Entry<String, Object> entry : variables.entrySet()) {
         String name = entry.getKey();
         Object value = entry.getValue();
         String type = determineType(value);
         String script = type + " " + name + " = (" + type + ") " + VariablesTransfer.class.getName() + ".getVariableValue(\"" + name + "\");";
         evaluateSnippet(jshell, accessDirectExecutionControl, script);
      }
   }

   private void pullVariables(JShell jshell, AccessDirectExecutionControl accessDirectExecutionControl, Bindings globalBindings, Bindings engineBindings) throws ScriptException {
      try {
         jshell.variables().forEach(varSnippet -> {
            String name = varSnippet.name();
            String script = VariablesTransfer.class.getName() + ".setVariableValue(\"" + name + "\", " + name + ");";
            try {
               evaluateSnippet(jshell, accessDirectExecutionControl, script);
               Object value = VariablesTransfer.getVariableValue(name);
               setBindingsValue(globalBindings, engineBindings, name, value);
            } catch (ScriptException e) {
               throw new ScriptRuntimeException(e);
            }
         });
      } catch (ScriptRuntimeException e) {
         throw (ScriptException) e.getCause();
      }

      VariablesTransfer.remove();
   }

   private void setBindingsValue(Bindings globalBindings, Bindings engineBindings, String name, Object value) {
      if (!engineBindings.containsKey(name) && globalBindings.containsKey(name)) {
         globalBindings.put(name, value);
      } else {
         engineBindings.put(name, value);
      }
   }

   private String determineType(Object value) {
      if (value == null) {
         return Object.class.getCanonicalName();
      }

      Class<?> clazz = value.getClass();
      while (clazz != null) {
         if (isValidType(clazz)) {
            return clazz.getCanonicalName();
         }
         for (Class<?> interfaceClazz : clazz.getInterfaces()) {
            if (isValidType(interfaceClazz)) {
               return interfaceClazz.getCanonicalName();
            }
         }
         clazz = clazz.getSuperclass();
      }

      return Object.class.getCanonicalName();
   }

   private boolean isValidType(Class<?> clazz) {
      if (clazz.getCanonicalName() == null) {
         return false;
      }

      return (clazz.getModifiers() & (Modifier.PRIVATE | Modifier.PROTECTED)) == 0;
   }

   private Map<String, Object> mergeBindings(Bindings... bindingsToMerge) {
      Map<String, Object> variables = new HashMap<>();

      for (Bindings bindings : bindingsToMerge) {
         if (bindings != null) {
            for (Map.Entry<String, Object> globalEntry : bindings.entrySet()) {
               variables.put(globalEntry.getKey(), globalEntry.getValue());
            }
         }
      }

      return variables;
   }

   private Object evaluateSnippets(JShell jshell, AccessDirectExecutionControl accessDirectExecutionControl) throws ScriptException {
      Object result = null;

      for (String snippetScript : snippets) {
         result = evaluateSnippet(jshell, accessDirectExecutionControl, snippetScript);
      }

      return result;
   }

   private Object evaluateSnippet(JShell jshell, AccessDirectExecutionControl accessDirectExecutionControl, String snippetScript) throws ScriptException {
      Object result = null;

      List<SnippetEvent> events = jshell.eval(snippetScript);

      for (SnippetEvent event : events) {
         if (event.status() == Snippet.Status.VALID && event.exception() == null) {
            result = accessDirectExecutionControl.getLastValue();
         } else {
            throwAsScriptException(jshell, event);
         }
      }
      return result;
   }

   private void throwAsScriptException(JShell jshell, SnippetEvent event) throws ScriptException {
      if (event.exception() != null) {
         JShellException exception = event.exception();
         String message = exception.getMessage() == null ? "" : ": " + exception.getMessage();
         if (exception instanceof EvalException) {
            EvalException evalException = (EvalException) exception;
            throw new ScriptException(evalException.getExceptionClassName() + message + "\n" + event.snippet().source());
         }
         throw new ScriptException(message + "\n" + event.snippet().source());
      }

      Snippet snippet = event.snippet();
      Optional<Diag> optionalDiag = jshell.diagnostics(snippet).findAny();
      if (optionalDiag.isPresent()) {
         Diag diag = optionalDiag.get();
         throw new ScriptException(diag.getMessage(null) + "\n" + snippet);
      }

      if (snippet instanceof DeclarationSnippet) {
         DeclarationSnippet declarationSnippet = (DeclarationSnippet) snippet;
         List<String> unresolvedDependencies = jshell.unresolvedDependencies(declarationSnippet).collect(Collectors.toList());
         if (!unresolvedDependencies.isEmpty()) {
            throw new ScriptException("Unresolved dependencies: " + unresolvedDependencies + "\n" + snippet);
         }
      }

      throw new ScriptException("Unknown error\n" + snippet);
   }

   @Override
   public ScriptEngine getEngine() {
      return engine;
   }

   private static class ScriptRuntimeException extends RuntimeException {
      public ScriptRuntimeException(ScriptException cause) {
         super(cause);
      }
   }

   private static class AccessDirectExecutionControl extends DirectExecutionControl {
      private Object lastValue;

      @Override
      protected String invoke(Method doitMethod) throws Exception {
         lastValue = doitMethod.invoke(null);
         return valueString(lastValue);
      }

      public Object getLastValue() {
         return lastValue;
      }
   }

   private static class AccessDirectExecutionControlProvider implements ExecutionControlProvider {
      private final AccessDirectExecutionControl accessDirectExecutionControl;

      AccessDirectExecutionControlProvider(AccessDirectExecutionControl accessDirectExecutionControl) {
         this.accessDirectExecutionControl = accessDirectExecutionControl;
      }

      @Override
      public String name() {
         return "accessdirect";
      }

      @Override
      public ExecutionControl generate(ExecutionEnv env, Map<String, String> parameters) {
         return accessDirectExecutionControl;
      }
   }
}
