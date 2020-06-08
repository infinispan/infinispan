package org.infinispan.scripting.engine.java;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import javax.script.SimpleScriptContext;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;

import org.infinispan.scripting.engine.java.constructor.ConstructorStrategy;
import org.infinispan.scripting.engine.java.constructor.DefaultConstructorStrategy;
import org.infinispan.scripting.engine.java.execution.DefaultExecutionStrategy;
import org.infinispan.scripting.engine.java.execution.ExecutionStrategy;
import org.infinispan.scripting.engine.java.execution.ExecutionStrategyFactory;
import org.infinispan.scripting.engine.java.name.DefaultNameStrategy;
import org.infinispan.scripting.engine.java.name.NameStrategy;

/**
 * Script engine to compile and run a Java class on the fly.
 * @author Eric Obermühlner
 */
public class JavaScriptEngine implements ScriptEngine, Compilable {

   private NameStrategy nameStrategy = new DefaultNameStrategy();
   private ConstructorStrategy constructorStrategy = DefaultConstructorStrategy.byDefaultConstructor();
   private ExecutionStrategyFactory executionStrategyFactory = clazz -> new DefaultExecutionStrategy(clazz);
   private Isolation isolation = Isolation.CallerClassLoader;

   private ScriptContext context = new SimpleScriptContext();

   private ClassLoader executionClassLoader = getClass().getClassLoader();

   /**
    * Sets the name strategy used to determine the Java class name from a script.
    *
    * @param nameStrategy the {@link NameStrategy} to use in this script engine
    */
   public void setNameStrategy(NameStrategy nameStrategy) {
      this.nameStrategy = nameStrategy;
   }

   /**
    * Sets the constructor strategy used to construct a Java instance of a class.
    *
    * @param constructorStrategy the {@link ConstructorStrategy} to use in this script engine
    */
   public void setConstructorStrategy(ConstructorStrategy constructorStrategy) {
      this.constructorStrategy = constructorStrategy;
   }

   /**
    * Sets the factory for the execution strategy used to execute a method of a class instance.
    *
    * @param executionStrategyFactory the {@link ExecutionStrategyFactory} to use in this script engine
    */
   public void setExecutionStrategyFactory(ExecutionStrategyFactory executionStrategyFactory) {
      this.executionStrategyFactory = executionStrategyFactory;
   }

   /**
    * Sets the {@link ClassLoader} used to load and execute the class.
    *
    * @param executionClassLoader the execution {@link ClassLoader}
    */
   public void setExecutionClassLoader(ClassLoader executionClassLoader) {
      this.executionClassLoader = executionClassLoader;
   }

   /**
    * Sets the isolation of the script.
    *
    * @param isolation the {@link Isolation}
    */
   public void setIsolation(Isolation isolation) {
      this.isolation = isolation;
   }

   @Override
   public ScriptContext getContext() {
      return context;
   }

   @Override
   public void setContext(ScriptContext context) {
      Objects.requireNonNull(context);
      this.context = context;
   }

   @Override
   public Bindings createBindings() {
      return new SimpleBindings();
   }

   @Override
   public Bindings getBindings(int scope) {
      return context.getBindings(scope);
   }

   @Override
   public void setBindings(Bindings bindings, int scope) {
      context.setBindings(bindings, scope);
   }

   @Override
   public void put(String key, Object value) {
      getBindings(ScriptContext.ENGINE_SCOPE).put(key, value);
   }

   @Override
   public Object get(String key) {
      return getBindings(ScriptContext.ENGINE_SCOPE).get(key);
   }

   @Override
   public Object eval(Reader reader) throws ScriptException {
      return eval(readScript(reader));
   }

   @Override
   public Object eval(String script) throws ScriptException {
      return eval(script, context);
   }

   @Override
   public Object eval(Reader reader, ScriptContext context) throws ScriptException {
      return eval(readScript(reader), context);
   }

   @Override
   public Object eval(String script, ScriptContext context) throws ScriptException {
      return eval(script, context.getBindings(ScriptContext.ENGINE_SCOPE));
   }

   @Override
   public Object eval(Reader reader, Bindings bindings) throws ScriptException {
      return eval(readScript(reader), bindings);
   }

   @Override
   public Object eval(String script, Bindings bindings) throws ScriptException {
      CompiledScript compile = compile(script);

      return compile.eval(bindings);
   }

   @Override
   public CompiledScript compile(Reader reader) throws ScriptException {
      return compile(readScript(reader));
   }

   @Override
   public JavaCompiledScript compile(String script) throws ScriptException {
      JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
      DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
      StandardJavaFileManager standardFileManager = compiler.getStandardFileManager(diagnostics, null, null);
      ClassLoader parentClassLoader = isolation == Isolation.CallerClassLoader ? executionClassLoader : null;
      MemoryFileManager memoryFileManager = new MemoryFileManager(standardFileManager, parentClassLoader);

      String fullClassName = nameStrategy.getFullName(script);
      String simpleClassName = NameStrategy.extractSimpleName(fullClassName);

      JavaFileObject scriptSource = memoryFileManager.createSourceFileObject(null, simpleClassName, script);

      JavaCompiler.CompilationTask task = compiler.getTask(null, memoryFileManager, diagnostics, null, null, Arrays.asList(scriptSource));
      if (!task.call()) {
         String message = diagnostics.getDiagnostics().stream()
               .map(d -> d.toString())
               .collect(Collectors.joining("\n"));
         throw new ScriptException(message);
      }

      ClassLoader classLoader = memoryFileManager.getClassLoader(StandardLocation.CLASS_OUTPUT);

      try {
         Class<?> clazz = classLoader.loadClass(fullClassName);
         Object instance = constructorStrategy.construct(clazz);
         ExecutionStrategy executionStrategy = executionStrategyFactory.create(clazz);
         return new JavaCompiledScript(this, clazz, instance, executionStrategy);
      } catch (ClassNotFoundException e) {
         throw new ScriptException(e);
      }
   }

   @Override
   public ScriptEngineFactory getFactory() {
      return new JavaScriptEngineFactory();
   }

   private String readScript(Reader reader) throws ScriptException {
      try {
         StringBuilder s = new StringBuilder();
         BufferedReader bufferedReader = new BufferedReader(reader);
         String line;
         while ((line = bufferedReader.readLine()) != null) {
            s.append(line);
            s.append("\n");
         }
         return s.toString();
      } catch (IOException e) {
         throw new ScriptException(e);
      }
   }

}
