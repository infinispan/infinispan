package org.infinispan.scripting.engine.java.constructor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import javax.script.ScriptException;

import org.junit.Test;

public class DefaultConstructorStrategyTest {
   @Test
   public void testByDefaultConstructor() throws ScriptException {
      DefaultConstructorStrategy constructorStrategy = DefaultConstructorStrategy.byDefaultConstructor();
      TestConstructor constructed = (TestConstructor) constructorStrategy.construct(TestConstructor.class);
      assertThat(constructed.message).isEqualTo("TestConstructor()");
   }

   @Test
   public void testByArgumentTypes() throws ScriptException {
      DefaultConstructorStrategy constructorStrategy = DefaultConstructorStrategy.byArgumentTypes(
            new Class<?>[]{String.class, int.class},
            "Hello", 42);
      TestConstructor constructed = (TestConstructor) constructorStrategy.construct(TestConstructor.class);
      assertThat(constructed.message).isEqualTo("TestConstructor(Hello,42)");
   }

   @Test
   public void testMatchingArgument() throws ScriptException {
      DefaultConstructorStrategy constructorStrategy = DefaultConstructorStrategy.byMatchingArguments(
            "Hello", 42);
      TestConstructor constructed = (TestConstructor) constructorStrategy.construct(TestConstructor.class);
      assertThat(constructed.message).isEqualTo("TestConstructor(Hello,42)");
   }

   @Test
   public void testMatchingArgumentPrimitives() throws ScriptException {
      DefaultConstructorStrategy constructorStrategy = DefaultConstructorStrategy.byMatchingArguments(
            123,
            12345L,
            (short) 12,
            (byte) 99,
            true,
            3.1416f,
            3.1415923,
            'x');
      TestConstructor constructed = (TestConstructor) constructorStrategy.construct(TestConstructor.class);
      assertThat(constructed.message).isEqualTo("TestConstructor(123,12345,12,99,true,3.1416,3.1415923,x)");
   }

   @Test
   public void testMatchingArgumentNull() throws ScriptException {
      DefaultConstructorStrategy constructorStrategy = DefaultConstructorStrategy.byMatchingArguments(
            null, 42);
      TestConstructor constructed = (TestConstructor) constructorStrategy.construct(TestConstructor.class);
      assertThat(constructed.message).isEqualTo("TestConstructor(null,42)");
   }

   @Test
   public void failMatchingArgumentPrimitiveNull() throws ScriptException {
      DefaultConstructorStrategy constructorStrategy = DefaultConstructorStrategy.byMatchingArguments(
            "Hello", null);
      assertThatThrownBy(() -> {
         constructorStrategy.construct(TestConstructor.class);
      }).isInstanceOf(ScriptException.class);
   }

   @Test
   public void failMatchingArgumentAmbiguousNull() throws ScriptException {
      DefaultConstructorStrategy constructorStrategy = DefaultConstructorStrategy.byMatchingArguments(
            "Hello", null, 42);
      assertThatThrownBy(() -> {
         constructorStrategy.construct(TestConstructor.class);
      }).isInstanceOf(ScriptException.class);
   }

   @Test
   public void failThrowException() throws ScriptException {
      DefaultConstructorStrategy constructorStrategy = DefaultConstructorStrategy.byMatchingArguments(
            new RuntimeException("some reason"));
      assertThatThrownBy(() -> {
         constructorStrategy.construct(TestConstructor.class);
      }).isInstanceOf(ScriptException.class);
   }

   private static class TestConstructor {
      public String message;

      public TestConstructor() {
         message = "TestConstructor()";
      }

      public TestConstructor(String text, int value) {
         message = "TestConstructor(" + text + "," + value + ")";
      }

      public TestConstructor(String text1, String text2, int value) {
         message = "TestConstructor(" + text1 + "," + text2 + "," + value + ")";
      }

      public TestConstructor(String text1, Long longValue, int value) {
         message = "TestConstructor(" + text1 + "," + longValue + "," + value + ")";
      }

      public TestConstructor(
            int intValue,
            long longValue,
            short shortValue,
            byte byteValue,
            boolean booleanValue,
            float floatValue,
            double doubleValue,
            char charValue) {
         message = "TestConstructor(" +
               intValue + "," +
               longValue + "," +
               shortValue + "," +
               byteValue + "," +
               booleanValue + "," +
               floatValue + "," +
               doubleValue + "," +
               charValue + ")";
      }

      private TestConstructor(RuntimeException exception) {
         throw exception;
      }
   }
}
