package org.infinispan.scripting.engine.java.execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.function.Supplier;

import javax.script.ScriptException;

import org.junit.Test;

public class DefaultExecutionStrategyTest {

   @Test
   public void testStaticMethodDefaultExecution() throws ScriptException {
      DefaultExecutionStrategy executionStrategy = new DefaultExecutionStrategy(TestStaticMethodExecution.class);
      Object result = executionStrategy.execute(null);

      assertThat(result).isEqualTo("static success");
   }

   @Test
   public void failStaticMethodDefaultExecution() throws ScriptException {
      DefaultExecutionStrategy executionStrategy = new DefaultExecutionStrategy(TestSupplierExecution.class);

      assertThatThrownBy(() -> {
         executionStrategy.execute(null);
      }).isInstanceOf(ScriptException.class);
   }

   @Test
   public void testSupplierDefaultExecution() throws ScriptException {
      DefaultExecutionStrategy executionStrategy = new DefaultExecutionStrategy(TestSupplierExecution.class);
      TestSupplierExecution instance = new TestSupplierExecution();
      Object result = executionStrategy.execute(instance);

      assertThat(result).isEqualTo("Supplier");
   }

   @Test
   public void testRunnableDefaultExecution() throws ScriptException {
      DefaultExecutionStrategy executionStrategy = new DefaultExecutionStrategy(TestRunnableExecution.class);
      TestRunnableExecution instance = new TestRunnableExecution();
      Object result = executionStrategy.execute(instance);

      assertThat(result).isNull();
      assertThat(instance.counter).isEqualTo(1);
   }

   @Test
   public void testMethodDefaultExecution() throws ScriptException {
      DefaultExecutionStrategy executionStrategy = new DefaultExecutionStrategy(TestMethodExecution.class);
      TestMethodExecution instance = new TestMethodExecution();
      Object result = executionStrategy.execute(instance);

      assertThat(result).isEqualTo("success");
   }

   @Test
   public void testNoMethodDefaultExecution() {
      DefaultExecutionStrategy executionStrategy = new DefaultExecutionStrategy(TestNoMethodExecution.class);
      TestNoMethodExecution instance = new TestNoMethodExecution();
      assertThatThrownBy(() -> {
         executionStrategy.execute(instance);
      }).isInstanceOf(ScriptException.class);
   }

   @Test
   public void testAmbiguousMethodDefaultExecution() {
      DefaultExecutionStrategy executionStrategy = new DefaultExecutionStrategy(TestAmbiguousMethodExecution.class);
      TestAmbiguousMethodExecution instance = new TestAmbiguousMethodExecution();
      assertThatThrownBy(() -> {
         executionStrategy.execute(instance);
      }).isInstanceOf(ScriptException.class);
   }

   @Test
   public void testThrowExceptionDefaultExecution() {
      DefaultExecutionStrategy executionStrategy = new DefaultExecutionStrategy(TestThrowExceptionExecution.class);
      TestThrowExceptionExecution instance = new TestThrowExceptionExecution();
      assertThatThrownBy(() -> {
         executionStrategy.execute(instance);
      }).isInstanceOf(ScriptException.class);
   }

   public static class TestStaticMethodExecution {
      private TestStaticMethodExecution() {
      }

      public static String getStaticSuccess() {
         return "static success";
      }

      public static String getStaticFailure(int value) {
         return "static failure-" + value;
      }

      public String getFailure(int value) {
         return "failure-" + value;
      }
   }

   public static class TestSupplierExecution implements Supplier<String> {
      @Override
      public String get() {
         return "Supplier";
      }
   }

   public static class TestRunnableExecution implements Runnable {
      public int counter = 0;

      @Override
      public void run() {
         counter++;
      }
   }

   public static class TestMethodExecution {
      public String getSuccess() {
         return "success";
      }

      public String getFailure(int value) {
         return "failure-" + value;
      }
   }

   public static class TestNoMethodExecution {
      public String getFailure(int value) {
         return "failure-" + value;
      }
   }

   public static class TestAmbiguousMethodExecution {
      public String getFailure1() {
         return "failure1";
      }

      public String getFailure2() {
         return "failure2";
      }
   }

   public static class TestThrowExceptionExecution {
      public void throwException() {
         throw new RuntimeException("some reason");
      }
   }
}
