package org.infinispan.commons.test.skip;

import java.util.Arrays;

import org.junit.Assume;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

public class SkipOnOsRule implements MethodRule {

   @Override
   public Statement apply(Statement base, FrameworkMethod method, Object target) {
      Statement result = base;
      SkipOnOs skipOsAnnotation = getSkipOsAnnotation(method);
      if (skipOsAnnotation != null && Arrays.asList(skipOsAnnotation.value()).contains(SkipOnOsUtils.getOs())) {
         result = new IgnoreStatement(method, skipOsAnnotation.value());
      }
      return result;
   }

   private SkipOnOs getSkipOsAnnotation(FrameworkMethod method) {
      return method.getAnnotation(SkipOnOs.class);
   }

   private static class IgnoreStatement extends Statement {

      private final FrameworkMethod method;
      private final SkipOnOs.OS [] skippedOses;

      IgnoreStatement(FrameworkMethod method, SkipOnOs.OS[] skippedOses) {
         this.method = method;
         this.skippedOses = skippedOses;
      }

      @Override
      public void evaluate() {
         String msg = "Skipping test " + method.getName() + " on " + Arrays.toString(skippedOses);
         System.out.println(msg);
         Assume.assumeTrue(msg, false);
      }
   }

}
