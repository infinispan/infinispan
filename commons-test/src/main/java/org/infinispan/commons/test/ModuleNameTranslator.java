package org.infinispan.commons.test;

public class ModuleNameTranslator {

   public static StringBuilder appendModule(StringBuilder result) {
      // JCache TCK tests are a special case
      if (getModuleSuffix().contains("tck-runner")) {
         if (result.indexOf("(") == -1) {
            result.append("(");
         } else {
            result.append(", ");
         }
         if (getInnerModuleSuffix().contains("remote")) {
            result.append("remote");
         } else if (getInnerModuleSuffix().contains("embedded")){
            result.append("embedded");
         } else {
            result.append("unknown");
         }
      }
      return result;
   }

   private static String getModuleSuffix() {
      // Remove the "-" from the beginning of the string
      return System.getProperty("infinispan.module-suffix").substring(1);
   }

   private static String getInnerModuleSuffix() {
      return System.getProperty("inner.infinispan.module-suffix");
   }
}
