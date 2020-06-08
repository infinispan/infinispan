package org.infinispan.scripting.engine.java;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import javax.script.SimpleScriptContext;

import org.infinispan.scripting.engine.java.constructor.DefaultConstructorStrategy;
import org.infinispan.scripting.engine.java.constructor.NullConstructorStrategy;
import org.infinispan.scripting.engine.java.execution.MethodExecutionStrategy;
import org.infinispan.scripting.engine.java.name.FixNameStrategy;
import org.junit.Test;

public class JavaScriptEngineTest {
   @Test
   public void testClassInDefaultPackage() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");

      Object result = engine.eval("" +
            "public class ClassInDefaultPackage {" +
            "   public String getMessage() {" +
            "       return getClass().getName();" +
            "   }" +
            "}");
      assertThat(result).isEqualTo("ClassInDefaultPackage");
   }

   @Test
   public void testClassInPackage() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");

      Object result = engine.eval("" +
            "package com.example;" +
            "public class ClassInPackage {" +
            "   public String getMessage() {" +
            "       return getClass().getName();" +
            "   }" +
            "}");
      assertThat(result).isEqualTo("com.example.ClassInPackage");
   }

   @Test
   public void testFixNameStrategy() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");
      JavaScriptEngine javaScriptEngine = (JavaScriptEngine) engine;

      javaScriptEngine.setNameStrategy(new FixNameStrategy("NeedFixName"));

      Object result = engine.eval("" +
            "public /*break*/ class NeedFixName {" +
            "   public String getMessage() {" +
            "       return getClass().getName();" +
            "   }" +
            "}");
      assertThat(result).isEqualTo("NeedFixName");
   }

   @Test
   public void testAutoCallSupplier() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");

      Object result = engine.eval("" +
            "public class Script implements java.util.function.Supplier<String> {" +
            "   @Override" +
            "   public String get() {" +
            "       return \"Hello\";" +
            "   }" +
            "   public int ignore() {" +
            "       return -1;" +
            "   }" +
            "}");
      assertThat(result).isEqualTo("Hello");
   }

   @Test
   public void testAutoCallRunnable() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");

      Object result = engine.eval("" +
            "public class Script implements java.lang.Runnable {" +
            "   public String message;" +
            "   @Override" +
            "   public void run() {" +
            "       message = \"Hello\";" +
            "   } " +
            "   public int ignore() {" +
            "       return -1;" +
            "   }" +
            "}");
      assertThat(result).isEqualTo(null);
      assertThat(engine.get("message")).isEqualTo("Hello");
   }

   @Test
   public void testAutoCallSingularMethod() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");

      Object result = engine.eval("" +
            "public class Script {" +
            "   public String getMessage() {" +
            "       return \"Hello\";" +
            "   }" +
            "}");
      assertThat(result).isEqualTo("Hello");
   }

   @Test
   public void failAutoCallSingularMethod() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");

      assertThatThrownBy(() -> {
         engine.eval("" +
               "public class Script {" +
               "}");
      }).isInstanceOf(ScriptException.class).hasMessageContaining("No method found to execute");

      assertThatThrownBy(() -> {
         engine.eval("" +
               "public class Script {" +
               "   public String getString() {" +
               "       return \"String\";" +
               "   }" +
               "   public int getInt() {" +
               "       return 42;" +
               "   }" +
               "}");
      }).isInstanceOf(ScriptException.class).hasMessageContaining("No method found to execute");
   }

   @Test
   public void testAutoCallSingularStaticMethod() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");

      Object result = engine.eval("" +
            "public class Script {" +
            "   public String getMessage(String message) {" +
            "       return \"Message: \" + message;" +
            "   }" +
            "   public static String main() {" +
            "       return new Script().getMessage(\"Hello\");" +
            "   }" +
            "}");
      assertThat(result).isEqualTo("Message: Hello");
   }

   @Test
   public void testMethodCallByArgumentTypes() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");
      JavaScriptEngine javaScriptEngine = (JavaScriptEngine) engine;

      javaScriptEngine.setExecutionStrategyFactory((clazz) -> {
         return MethodExecutionStrategy.byArgumentTypes(
               clazz,
               "getMessage",
               new Class[]{String.class, int.class},
               "Hello", 42);
      });

      Object result = engine.eval("" +
            "public class Script {" +
            "   public String getMessage(String message, int value) {" +
            "       return \"Message: \" + message + value;" +
            "   } " +
            "}");
      assertThat(result).isEqualTo("Message: Hello42");
   }

   @Test
   public void testMethodCallByMatchingArgument() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");
      JavaScriptEngine javaScriptEngine = (JavaScriptEngine) engine;

      javaScriptEngine.setExecutionStrategyFactory((clazz) -> {
         return MethodExecutionStrategy.byMatchingArguments(
               clazz,
               "getMessage",
               "Hello", 42);
      });

      Object result = engine.eval("" +
            "public class Script {" +
            "   public String getMessage(String message, int value) {" +
            "       return \"Message: \" + message + value;" +
            "   } " +
            "}");
      assertThat(result).isEqualTo("Message: Hello42");
   }

   @Test
   public void testMethodCallByMatchingArgumentAssignable() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");
      JavaScriptEngine javaScriptEngine = (JavaScriptEngine) engine;

      javaScriptEngine.setExecutionStrategyFactory((clazz) -> {
         return MethodExecutionStrategy.byMatchingArguments(
               clazz,
               "getMessage",
               "Hello", 42);
      });

      Object result = engine.eval("" +
            "public class Script {" +
            "   public String getMessage(Object message, int value) {" +
            "       return \"Message: \" + message + value;" +
            "   } " +
            "}");
      assertThat(result).isEqualTo("Message: Hello42");
   }

   @Test
   public void testMethodCallByMatchingArgumentNull() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");
      JavaScriptEngine javaScriptEngine = (JavaScriptEngine) engine;

      javaScriptEngine.setExecutionStrategyFactory((clazz) -> {
         return MethodExecutionStrategy.byMatchingArguments(
               clazz,
               "getMessage",
               null, 42);
      });

      Object result = engine.eval("" +
            "public class Script {" +
            "   public String getMessage(String message, int value) {" +
            "       return \"Message: \" + message + value;" +
            "   } " +
            "}");
      assertThat(result).isEqualTo("Message: null42");
   }

   @Test
   public void failMethodCallByMatchingArgument() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");
      JavaScriptEngine javaScriptEngine = (JavaScriptEngine) engine;

      javaScriptEngine.setExecutionStrategyFactory((clazz) -> {
         return MethodExecutionStrategy.byMatchingArguments(
               clazz,
               "getMessage",
               "Hello", 42);
      });

      assertThatThrownBy(() -> {
         engine.eval("" +
               "public class Script {" +
               "   public String getMessage(int value1, int value2) {" +
               "       return \"Message: \" + value1 + value2;" +
               "   } " +
               "}");
      }).isInstanceOf(ScriptException.class);
   }

   @Test
   public void failMethodCallByMatchingArgumentNullPrimitive() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");
      JavaScriptEngine javaScriptEngine = (JavaScriptEngine) engine;

      javaScriptEngine.setExecutionStrategyFactory((clazz) -> {
         return MethodExecutionStrategy.byMatchingArguments(
               clazz,
               "getMessage",
               null, null);
      });

      assertThatThrownBy(() -> {
         engine.eval("" +
               "public class Script {" +
               "   public String getMessage(String message, int value) {" +
               "       return \"Message: \" + message + value;" +
               "   } " +
               "}");
      }).isInstanceOf(ScriptException.class);
   }

   @Test
   public void testConstructorWithArguments() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");
      JavaScriptEngine javaScriptEngine = (JavaScriptEngine) engine;

      javaScriptEngine.setConstructorStrategy(DefaultConstructorStrategy.byArgumentTypes(new Class<?>[]{String.class, int.class}, "Hello", 42));

      Object result = engine.eval("" +
            "public class Script {" +
            "   private final String message;" +
            "   private final int value;" +
            "   public Script(String message, int value) {" +
            "       this.message = message;" +
            "       this.value = value;" +
            "   }" +
            "   public String getMessage() {" +
            "       return \"Message: \" + message + value;" +
            "   }" +
            "}");
      assertThat(result).isEqualTo("Message: Hello42");
   }

   @Test
   public void testConstructorNull() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");
      JavaScriptEngine javaScriptEngine = (JavaScriptEngine) engine;

      javaScriptEngine.setConstructorStrategy(new NullConstructorStrategy());

      Object result = engine.eval("" +
            "public class Script {" +
            "   private static String message = \"Unknown\";" +
            "   private static int value = 99;" +
            "   private Script() {" +
            "   }" +
            "   public static String getMessage() {" +
            "       return \"Message: \" + message + value;" +
            "   }" +
            "}");
      assertThat(result).isEqualTo("Message: Unknown99");
   }

   @Test
   public void testBindings() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");

      engine.put("message", "Hello");
      engine.put("counter", 42);

      Object result = engine.eval("" +
            "public class Script {" +
            "   public static String message;" +
            "   public int counter;" +
            "   public String getMessage() {" +
            "       return message + counter++;" +
            "   }" +
            "}");
      assertThat(result).isEqualTo("Hello42");
      assertThat(engine.get("counter")).isEqualTo(43);
   }

   @Test
   public void failBindingsPrivate() {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");

      engine.put("message", "Hello");

      assertThatThrownBy(() -> {
         engine.eval("" +
               "public class Script {" +
               "   private String message;" +
               "   public String getMessage() {" +
               "       return message;" +
               "   }" +
               "}");
      }).isInstanceOf(ScriptException.class);
   }

   @Test
   public void failBindingsStaticPrivate() {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");

      engine.put("message", "Hello");

      assertThatThrownBy(() -> {
         engine.eval("" +
               "public class Script {" +
               "   private static String message;" +
               "   public String getMessage() {" +
               "       return message;" +
               "   }" +
               "}");
      }).isInstanceOf(ScriptException.class);
   }

   @Test
   public void testGlobalBindings() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      manager.put("message", "Hello");
      manager.put("counter", 42);

      ScriptEngine engine = manager.getEngineByName("java");

      Object result = engine.eval("" +
            "public class Script {" +
            "   public static String message;" +
            "   public int counter;" +
            "   public String getMessage() {" +
            "       return message + counter++;" +
            "   }" +
            "}");
      assertThat(result).isEqualTo("Hello42");
      assertThat(manager.get("counter")).isEqualTo(43);
      assertThat(engine.get("counter")).isNull();
      assertThat(engine.getBindings(ScriptContext.ENGINE_SCOPE).get("counter")).isNull();
      assertThat(engine.getBindings(ScriptContext.GLOBAL_SCOPE).get("counter")).isEqualTo(43);
   }

   @Test
   public void testCompilable() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");
      assertThat(engine).isInstanceOf(Compilable.class);

      String script = "" +
            "public class CompiledScript {" +
            "   public static String message;" +
            "   public int counter;" +
            "   public String getMessage() {" +
            "       return \"Message: \" + message + counter++;" +
            "   }" +
            "}";

      Compilable compiler = (Compilable) engine;
      CompiledScript compiledScript = compiler.compile(script);
      assertThat(compiledScript.getEngine()).isSameAs(engine);

      JavaCompiledScript javaCompiledScript = (JavaCompiledScript) compiledScript;
      assertThat(javaCompiledScript.getCompiledClass().getName()).isEqualTo("CompiledScript");
      assertThat(javaCompiledScript.getCompiledInstance()).isNotNull();
      assertThat(javaCompiledScript.getCompiledInstance().getClass().getName()).isEqualTo("CompiledScript");

      for (int i = 0; i < 2; i++) {
         Bindings bindings = engine.createBindings();

         bindings.put("message", "Hello-");
         bindings.put("counter", i);
         Object result = compiledScript.eval(bindings);

         assertThat(result).isEqualTo("Message: Hello-" + i);
         assertThat(bindings.get("counter")).isEqualTo(i + 1);
      }
   }

   @Test
   public void testCompilableReader() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");
      assertThat(engine).isInstanceOf(Compilable.class);

      StringReader script = new StringReader("" +
            "public class CompiledScript {" +
            "   public static String message;" +
            "   public int counter;" +
            "   public String getMessage() {" +
            "       return \"Message: \" + message + counter++;" +
            "   }" +
            "}");

      Compilable compiler = (Compilable) engine;
      CompiledScript compiledScript = compiler.compile(script);
      assertThat(compiledScript.getEngine()).isSameAs(engine);

      JavaCompiledScript javaCompiledScript = (JavaCompiledScript) compiledScript;
      assertThat(javaCompiledScript.getCompiledClass().getName()).isEqualTo("CompiledScript");
      assertThat(javaCompiledScript.getCompiledInstance()).isNotNull();
      assertThat(javaCompiledScript.getCompiledInstance().getClass().getName()).isEqualTo("CompiledScript");

      for (int i = 0; i < 2; i++) {
         Bindings bindings = engine.createBindings();

         bindings.put("message", "Hello-");
         bindings.put("counter", i);
         Object result = compiledScript.eval(bindings);

         assertThat(result).isEqualTo("Message: Hello-" + i);
         assertThat(bindings.get("counter")).isEqualTo(i + 1);
      }
   }

   @Test
   public void testCompilableExecutionStrategy() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");
      Compilable compiler = (Compilable) engine;

      CompiledScript compiledScript = compiler.compile("" +
            "public class Script {" +
            "   public String getMessage(String message, int value) {" +
            "       return \"Message: \" + message + value;" +
            "   } " +
            "}");
      JavaCompiledScript javaCompiledScript = (JavaCompiledScript) compiledScript;

      javaCompiledScript.setExecutionStrategy(MethodExecutionStrategy.byMatchingArguments(
            javaCompiledScript.getCompiledClass(),
            "getMessage",
            "Hello", 42));

      Object result = javaCompiledScript.eval();

      assertThat(result).isEqualTo("Message: Hello42");
   }

   @Test
   public void testPrivateStaticClass() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");

      Object result = engine.eval("" +
            "public class Script {" +
            "   public Object getMessage() {" +
            "       return new ScriptPrivateClass();" +
            "   }" +
            "   private static class ScriptPrivateClass {" +
            "       public String toString() {" +
            "           return \"Hello\";" +
            "       }" +
            "   }" +
            "}");
      assertThat(result.getClass().getSimpleName()).isEqualTo("ScriptPrivateClass");
      assertThat(result.toString()).isEqualTo("Hello");
   }

   @Test
   public void testIsolationCallerClassLoaderPublicClass() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");
      JavaScriptEngine javaScriptEngine = (JavaScriptEngine) engine;

      JavaCompiledScript compiledScript = javaScriptEngine.compile("" +
            "import ch.obermuhlner.scriptengine.java.JavaScriptEngineTest.PublicClass;" +
            "public class Script {" +
            "   public Object getMessage() {" +
            "       PublicClass result = new PublicClass();" +
            "       result.message = \"Hello\";" +
            "       return result;" +
            "   }" +
            "}");

      Object result = compiledScript.eval();

      assertThat(result).isInstanceOf(PublicClass.class);
      assertThat(((PublicClass) result).message).isEqualTo("Hello");
   }

   @Test
   public void testIsolationIsolatedPublicClass() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");
      JavaScriptEngine javaScriptEngine = (JavaScriptEngine) engine;

      javaScriptEngine.setIsolation(Isolation.IsolatedClassLoader);

      JavaCompiledScript compiledScript = javaScriptEngine.compile("" +
            "import ch.obermuhlner.scriptengine.java.JavaScriptEngineTest.PublicClass;" +
            "public class Script {" +
            "   public Object getMessage() {" +
            "       PublicClass result = new PublicClass();" +
            "       result.message = \"Hello\";" +
            "       return result;" +
            "   }" +
            "}");

      assertThatThrownBy(() -> {
         compiledScript.eval();
      }).isInstanceOf(ScriptException.class);
   }

   @Test
   public void testEvalReader() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");

      Reader reader = new StringReader("" +
            "public class Script {" +
            "   public int getValue() {" +
            "       return 1234;" +
            "   }" +
            "}");
      Object result = engine.eval(reader);
      assertThat(result).isEqualTo(1234);
   }

   @Test
   public void testEvalReaderContext() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");

      ScriptContext context = new SimpleScriptContext();
      context.getBindings(ScriptContext.ENGINE_SCOPE).put("alpha", 1000);

      Reader reader = new StringReader("" +
            "public class Script {" +
            "   public int alpha;" +
            "   public int getValue() {" +
            "       return alpha + 999;" +
            "   }" +
            "}");
      Object result = engine.eval(reader, context);
      assertThat(result).isEqualTo(1999);
   }

   @Test
   public void testEvalReaderBindings() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");

      SimpleBindings bindings = new SimpleBindings();
      bindings.put("alpha", 1000);

      Reader reader = new StringReader("" +
            "public class Script {" +
            "   public int alpha;" +
            "   public int getValue() {" +
            "       return alpha + 321;" +
            "   }" +
            "}");
      Object result = engine.eval(reader, bindings);
      assertThat(result).isEqualTo(1321);
   }

   @Test
   public void failEvalReader() {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");

      Reader reader = new FailReader();
      assertThatThrownBy(() -> {
         engine.eval(reader);
      }).isInstanceOf(ScriptException.class);
   }

   @Test
   public void testSetGetContext() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");

      assertThat(engine.getContext()).isNotNull();

      SimpleScriptContext context = new SimpleScriptContext();
      engine.setContext(context);
      assertThat(engine.getContext()).isSameAs(context);

      assertThatThrownBy(() -> {
         engine.setContext(null);
      }).isInstanceOf(NullPointerException.class);
   }

   @Test
   public void testCreateBindings() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");

      assertThat(engine.createBindings()).isNotNull();
   }

   @Test
   public void testSetBindings() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");

      engine.setBindings(null, ScriptContext.GLOBAL_SCOPE);

      assertThatThrownBy(() -> {
         engine.setBindings(null, ScriptContext.ENGINE_SCOPE);
      }).isInstanceOf(NullPointerException.class);

      assertThatThrownBy(() -> {
         engine.setBindings(new SimpleBindings(), -999);
      }).isInstanceOf(IllegalArgumentException.class);
   }

   @Test
   public void testGetFactory() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");

      ScriptEngineFactory factory = engine.getFactory();
      assertThat(factory.getClass()).isSameAs(JavaScriptEngineFactory.class);
   }

   @Test
   public void failSyntaxError() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("java");

      assertThatThrownBy(() -> {
         engine.eval("" +
               "public class Script {" +
               "   XXX" +
               "}");
      }).isInstanceOf(ScriptException.class);
   }

   public static class PublicClass {
      public String message;
   }

   public static class FailReader extends Reader {
      public FailReader() {
      }

      @Override
      public int read(char[] cbuf, int off, int len) throws IOException {
         throw new IOException("FailReader: always fails in read()");
      }

      @Override
      public void close() throws IOException {
         throw new IOException("FailReader: always fails in close()");
      }
   }
}
