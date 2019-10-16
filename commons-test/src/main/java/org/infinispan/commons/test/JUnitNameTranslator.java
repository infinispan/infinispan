package org.infinispan.commons.test;

import static org.infinispan.commons.test.ModuleNameTranslator.appendModule;

import org.junit.runner.Description;

public class JUnitNameTranslator {

   public static String translateTestName(Description description) {
      StringBuilder result = new StringBuilder();
      result.append(description.getMethodName());
      // JCache TCK tests are a special case
      appendModule(result);
      if (result.indexOf("(") != -1) {
         result.append(")");
      }
      return result.toString();
   }
}
