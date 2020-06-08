package org.infinispan.scripting.engines.jshell;

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

import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Eric Obermühlner
 */
public class JShellScriptEngineTest {
   @Test
   public void testEmpty() throws ScriptException {
      assertScript("", null);
   }

   @Test
   public void testSimple() throws ScriptException {
      assertScript("2+3", 5);
   }

   @Test
   public void testSimpleDeclareVariable() throws ScriptException {
      assertScript("var alpha = 123", 123);
   }

   @Ignore("lastValue() has no access to default value of declarations")
   @Test
   public void testSimpleDeclareIntVariable() throws ScriptException {
      assertScript("int alpha", 123);
   }

   @Test
   public void testFailUnknownVariable() {
      assertThatThrownBy(() -> {
         assertScript("unknown", null);
      }).isInstanceOf(ScriptException.class).hasMessageContaining("unknown");
   }

   @Test
   public void testFailIncompleteScript() {
      assertThatThrownBy(() -> {
         assertScript("foo(", null);
      }).isInstanceOf(ScriptException.class).hasMessageContaining("Incomplete");
   }

   @Test
   public void testFailSameVariable() {
      assertThatThrownBy(() -> {
         assertScript("" +
                     "var alpha = 0;" +
                     "var alpha = 1;",
               null);
      }).isInstanceOf(ScriptException.class).hasMessageContaining("alpha");
   }

   @Test
   public void testFailEvalDivByZero() {
      assertThatThrownBy(() -> {
         assertScript("1/0", null);
      }).isInstanceOf(ScriptException.class).hasMessageContaining("ArithmeticException");
   }

   @Test
   public void testFailEvalNullPointerException() {
      assertThatThrownBy(() -> {
         assertScript("" +
                     "Object foo = null;" +
                     "foo.toString()",
               null);
      }).isInstanceOf(ScriptException.class).hasMessageContaining("NullPointerException");
   }

   @Test
   public void testBindingsExistingVariable() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("jshell");
      engine.put("alpha", 2);
      engine.put("beta", 3);
      engine.put("gamma", 0);

      Object result = engine.eval("gamma = alpha + beta");

      assertThat(result).isEqualTo(5);
      assertThat(engine.get("gamma")).isEqualTo(5);
   }

   @Test
   public void testBindingsNewVariable() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("jshell");
      engine.put("alpha", 2);
      engine.put("beta", 3);

      Object result = engine.eval("var gamma = alpha + beta");
      assertThat(result).isEqualTo(5);
      assertThat(engine.get("gamma")).isEqualTo(5);
   }

   @Test
   public void testBindingsMultipleEval() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("jshell");

      engine.put("alpha", 2);
      engine.put("beta", 3);
      engine.put("gamma", 0);

      Object result;
      result = engine.eval("gamma = alpha + beta");
      assertThat(result).isEqualTo(5);
      assertThat(engine.get("gamma")).isEqualTo(5);

      result = engine.eval("gamma = alpha + beta + gamma");
      assertThat(result).isEqualTo(10);
      assertThat(engine.get("gamma")).isEqualTo(10);

      engine.put("alpha", "aaa");
      engine.put("beta", "bbb");
      engine.put("gamma", "");

      result = engine.eval("gamma = alpha + beta");
      assertThat(result).isEqualTo("aaabbb");
      assertThat(engine.get("gamma")).isEqualTo("aaabbb");

      result = engine.eval("gamma = alpha + beta + gamma");
      assertThat(result).isEqualTo("aaabbbaaabbb");
      assertThat(engine.get("gamma")).isEqualTo("aaabbbaaabbb");

   }

   @Test
   public void testBindingsGlobalVariable() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("jshell");
      Bindings globalBindings = engine.getBindings(ScriptContext.GLOBAL_SCOPE);
      globalBindings.put("alpha", 2);
      globalBindings.put("beta", 3);
      globalBindings.put("gamma", 0);

      Object result = engine.eval("gamma = alpha + beta");
      assertThat(result).isEqualTo(5);
      assertThat(engine.get("gamma")).isEqualTo(null);
      assertThat(globalBindings.get("gamma")).isEqualTo(5);
   }

   @Test
   public void testBindingsOverrideGlobalVariable() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("jshell");
      Bindings globalBindings = engine.getBindings(ScriptContext.GLOBAL_SCOPE);
      globalBindings.put("alpha", 2);
      globalBindings.put("beta", 3);
      globalBindings.put("gamma", 0);

      engine.put("gamma", 999);

      assertThat(engine.get("gamma")).isEqualTo(999);
      assertThat(globalBindings.get("gamma")).isEqualTo(0);

      Object result = engine.eval("gamma = alpha + beta");
      assertThat(result).isEqualTo(5);
      assertThat(engine.get("gamma")).isEqualTo(5);
      assertThat(globalBindings.get("gamma")).isEqualTo(0);
   }

   @Test
   public void testBindingsPublicClass() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("jshell");
      PublicClass publicClass = new PublicClass();
      publicClass.message = "hello";
      engine.put("alpha", publicClass);

      Object result = engine.eval(
            PublicClass.class.getCanonicalName() + " beta = alpha;" +
                  "var message = alpha.message");
      assertThat(result).isEqualTo("hello");
      assertThat(engine.get("alpha")).isSameAs(publicClass);
      assertThat(engine.get("message")).isEqualTo("hello");
   }

   @Test
   public void testBindingsPrivateClassFail() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("jshell");
      PrivateClass privateClass = new PrivateClass();
      engine.put("alpha", privateClass);

      assertThatThrownBy(() -> {
         Object result = engine.eval(PrivateClass.class.getName() + " beta = alpha");
      }).isInstanceOf(ScriptException.class).hasMessageContaining("PrivateClass");
   }

   @Test
   public void testBindingsPrivateClassAsObject() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("jshell");
      PrivateClass privateClass = new PrivateClass();
      engine.put("alpha", privateClass);

      Object result = engine.eval("Object beta = alpha");
      assertThat(engine.get("alpha")).isSameAs(privateClass);
      assertThat(engine.get("beta")).isSameAs(privateClass);
   }

   @Test
   public void testBindingsProtectedClassFail() {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("jshell");
      ProtectedClass protectedClass = new ProtectedClass();
      engine.put("alpha", protectedClass);

      assertThatThrownBy(() -> {
         Object result = engine.eval(ProtectedClass.class.getName() + " beta = alpha");
      }).isInstanceOf(ScriptException.class).hasMessageContaining("ProtectedClass");
   }

   @Test
   public void testBindingsProtectedClassAsObject() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("jshell");
      ProtectedClass protectedClass = new ProtectedClass();
      engine.put("alpha", protectedClass);

      Object result = engine.eval("Object beta = alpha");
      assertThat(engine.get("alpha")).isSameAs(protectedClass);
      assertThat(engine.get("beta")).isSameAs(protectedClass);
   }

   @Test
   public void testBindingsPrivateClassAsInterface() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("jshell");
      PrivateClassWithMyInterface privateClassWithMyInterface = new PrivateClassWithMyInterface();
      engine.put("alpha", privateClassWithMyInterface);

      Object result = engine.eval(
            MyInterface.class.getCanonicalName() + " beta = alpha;" +
                  "alpha.getMessage()");
      assertThat(result).isEqualTo("my message");
      assertThat(engine.get("alpha")).isSameAs(privateClassWithMyInterface);
      assertThat(engine.get("beta")).isSameAs(privateClassWithMyInterface);
   }

   @Test
   public void testBindingsNullAsObject() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("jshell");
      engine.put("alpha", null);

      Object result = engine.eval("Object beta = alpha");
      assertThat(engine.get("alpha")).isNull();
      assertThat(engine.get("beta")).isNull();
   }

   @Test
   public void testBindingsAnonymousClassAsObject() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("jshell");
      Object anonymous = new Object() {
      };
      System.out.println(anonymous.getClass().getCanonicalName());
      engine.put("alpha", anonymous);

      Object result = engine.eval("Object beta = alpha");
      assertThat(engine.get("alpha")).isSameAs(anonymous);
      assertThat(engine.get("beta")).isSameAs(anonymous);
   }

   @Test
   public void testBindingsPutIllegalVariable() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("jshell");

      assertThatThrownBy(() -> {
         engine.put("illegal with spaces", 2);
         Object result = engine.eval("1234");
      }).isInstanceOf(ScriptException.class);
   }

   @Test
   public void testBindingsGetIllegalVariable() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("jshell");

      assertThat(engine.get("illegal with spaces")).isNull();
   }

   @Test
   public void testCompilable() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("jshell");
      assertThat(engine).isInstanceOf(Compilable.class);

      String script = "alpha + beta";

      Compilable compiler = (Compilable) engine;
      CompiledScript compiledScript = compiler.compile(script);
      assertThat(compiledScript.getEngine()).isSameAs(engine);

      for (int i = 0; i < 2; i++) {
         Bindings bindings = engine.createBindings();

         bindings.put("alpha", 2);
         bindings.put("beta", 3);
         Object result = compiledScript.eval(bindings);
         assertThat(result).isEqualTo(5);
      }

      for (int i = 0; i < 2; i++) {
         Bindings bindings = engine.createBindings();

         bindings.put("alpha", "aaa");
         bindings.put("beta", "bbb");
         Object result = compiledScript.eval(bindings);
         assertThat(result).isEqualTo("aaabbb");
      }
   }

   @Test
   public void testCompilableReader() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("jshell");
      assertThat(engine).isInstanceOf(Compilable.class);

      String script = "alpha + beta";
      StringReader scriptReader = new StringReader(script);

      Compilable compiler = (Compilable) engine;
      CompiledScript compiledScript = compiler.compile(scriptReader);
      assertThat(compiledScript.getEngine()).isSameAs(engine);

      for (int i = 0; i < 2; i++) {
         Bindings bindings = engine.createBindings();

         bindings.put("alpha", 2);
         bindings.put("beta", 3);
         Object result = compiledScript.eval(bindings);
         assertThat(result).isEqualTo(5);
      }

      for (int i = 0; i < 2; i++) {
         Bindings bindings = engine.createBindings();

         bindings.put("alpha", "aaa");
         bindings.put("beta", "bbb");
         Object result = compiledScript.eval(bindings);
         assertThat(result).isEqualTo("aaabbb");
      }
   }

   @Test
   public void testEvalReader() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("jshell");

      Reader reader = new StringReader("1234");
      Object result = engine.eval(reader);
      assertThat(result).isEqualTo(1234);
   }

   @Test
   public void testEvalReaderContext() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("jshell");

      ScriptContext context = new SimpleScriptContext();
      context.getBindings(ScriptContext.ENGINE_SCOPE).put("alpha", 1000);
      Reader reader = new StringReader("alpha+999");
      Object result = engine.eval(reader, context);
      assertThat(result).isEqualTo(1999);
   }

   @Test
   public void testEvalReaderBindings() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("jshell");

      SimpleBindings bindings = new SimpleBindings();
      bindings.put("alpha", 1000);
      Reader reader = new StringReader("alpha+321");
      Object result = engine.eval(reader, bindings);
      assertThat(result).isEqualTo(1321);
   }

   @Test
   public void testEvalReaderFail() {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("jshell");

      Reader reader = new FailReader();
      assertThatThrownBy(() -> {
         Object result = engine.eval(reader);
      }).isInstanceOf(ScriptException.class);
   }

   @Test
   public void testSetGetContext() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("jshell");

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
      ScriptEngine engine = manager.getEngineByName("jshell");

      assertThat(engine.createBindings()).isNotNull();
   }

   @Test
   public void testSetBindings() throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("jshell");

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
      ScriptEngine engine = manager.getEngineByName("jshell");

      ScriptEngineFactory factory = engine.getFactory();
      assertThat(factory.getClass()).isSameAs(JShellScriptEngineFactory.class);
   }

   private void assertScript(String script, Object expectedResult) throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("jshell");
      Object result = engine.eval(script);
      assertThat(result).isEqualTo(expectedResult);
   }

   public static class PublicClass {
      public String message;
   }

   private static class PrivateClass {
   }

   public interface MyInterface {
      default String getMessage() {
         return "my message";
      }
   }

   private static class PrivateClassWithMyInterface implements MyInterface {
   }

   protected static class ProtectedClass {
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
