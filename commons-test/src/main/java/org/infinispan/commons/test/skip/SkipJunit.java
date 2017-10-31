package org.infinispan.commons.test.skip;

import java.util.Arrays;
import java.util.Objects;

import org.junit.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Use as a {@code @Rule} or {@code @ClassRule} to skip all methods in a class on some OSes.
 */
public class SkipJunit implements TestRule {
   private final OS[] oses;

   public SkipJunit(OS... oses) {
      this.oses = Objects.requireNonNull(oses);
   }

   @Override
   public Statement apply(Statement base, Description description) {
      OS os = OS.getCurrentOs();
      if (!Arrays.asList(oses).contains(os))
         return base;

      return new IgnoreStatement(description, os);
   }

   /**
    * Use inside a method to skip that particular method on some OSes.
    */
   public static void skipOnOS(OS... oses) {
      OS os = OS.getCurrentOs();
      if (Arrays.asList(oses).contains(os))
         throw new AssumptionViolatedException("Skipping test on " + os);
   }

   private static class IgnoreStatement extends Statement {

      private final Description method;

      private final OS os;

      IgnoreStatement(Description method, OS os) {
         this.method = method;
         this.os = os;
      }

      @Override
      public void evaluate() {
         String msg = "Skipping test " + method.getDisplayName() + " on " + os;
         System.out.println(msg);
         throw new AssumptionViolatedException(msg);
      }
   }

}
