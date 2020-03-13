package org.infinispan.commons.jdkspecific;

/**
 * JDK 10+ implementation
 *
 * @author Tristan Tarrant
 */
public class ProcessorInfo {

   private ProcessorInfo() {
   }

   public static int availableProcessors() {
      int javaProcs = Runtime.getRuntime().availableProcessors();
      int userProcs = Integer.getInteger("infinispan.activeprocessorcount", javaProcs);
      return Math.min(userProcs, javaProcs);
   }
}
