package org.infinispan.scripting.engine.java.execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Method;

import javax.script.ScriptException;

import org.junit.Test;

public class MethodExecutionStrategyTest {
   @Test
   public void testByMethod() throws ScriptException, NoSuchMethodException {
      Method method = TestMethod.class.getMethod("doSomething", String.class, int.class);
      MethodExecutionStrategy methodExecutionStrategy = MethodExecutionStrategy.byMethod(method, "Hello", 42);
      TestMethod instance = new TestMethod();
      Object result = methodExecutionStrategy.execute(instance);
      assertThat(result).isEqualTo("doSomething(Hello,42)");
   }

   @Test
   public void testByArgumentTypes() throws ScriptException {
      MethodExecutionStrategy methodExecutionStrategy = MethodExecutionStrategy.byArgumentTypes(
            TestMethod.class,
            "doSomething",
            new Class<?>[]{String.class, int.class},
            "Hello", 42);
      TestMethod instance = new TestMethod();
      Object result = methodExecutionStrategy.execute(instance);
      assertThat(result).isEqualTo("doSomething(Hello,42)");
   }

   @Test
   public void testByMatchingArguments() throws ScriptException {
      MethodExecutionStrategy methodExecutionStrategy = MethodExecutionStrategy.byMatchingArguments(
            TestMethod.class,
            "doSomething",
            "Hello", 42);
      TestMethod instance = new TestMethod();
      Object result = methodExecutionStrategy.execute(instance);
      assertThat(result).isEqualTo("doSomething(Hello,42)");
   }

   @Test
   public void testNullByMatchingArguments() throws ScriptException {
      MethodExecutionStrategy methodExecutionStrategy = MethodExecutionStrategy.byMatchingArguments(
            TestMethod.class,
            "doSomething",
            null, 42);
      TestMethod instance = new TestMethod();
      Object result = methodExecutionStrategy.execute(instance);
      assertThat(result).isEqualTo("doSomething(null,42)");
   }

   @Test
   public void failUnknownByMatchingArguments() throws ScriptException {
      assertThatThrownBy(() -> {
         MethodExecutionStrategy.byMatchingArguments(
               TestMethod.class,
               "unknown",
               "Hello", 42);
      }).isInstanceOf(ScriptException.class);
   }

   @Test
   public void failNoMatchByMatchingArguments() throws ScriptException {
      assertThatThrownBy(() -> {
         MethodExecutionStrategy.byMatchingArguments(
               TestMethod.class,
               "doSomething",
               1, 2, 3, 4);
      }).isInstanceOf(ScriptException.class);
   }

   @Test
   public void failAmbiguousByMatchingArguments() throws ScriptException {
      assertThatThrownBy(() -> {
         MethodExecutionStrategy.byMatchingArguments(
               TestMethod.class,
               "doSomething",
               null, null, 42);
      }).isInstanceOf(ScriptException.class);
   }

   @Test
   public void testThrowException() throws ScriptException, NoSuchMethodException {
      Method method = TestMethod.class.getMethod("throwException");
      MethodExecutionStrategy methodExecutionStrategy = MethodExecutionStrategy.byMethod(method);
      TestMethod instance = new TestMethod();
      assertThatThrownBy(() -> {
         methodExecutionStrategy.execute(instance);
      }).isInstanceOf(ScriptException.class);
   }

   public static class TestMethod {
      public String doSomething(String text, int value) {
         return "doSomething(" + text + "," + value + ")";
      }

      public String doSomething(String text1, String text2, int value) {
         return "doSomething(" + text1 + "," + text2 + "," + value + ")";
      }

      public String doSomething(String text, Long longValue, int value) {
         return "doSomething(" + text + "," + longValue + "," + value + ")";
      }

      public String throwException() {
         throw new RuntimeException("some reason");
      }
   }

}
