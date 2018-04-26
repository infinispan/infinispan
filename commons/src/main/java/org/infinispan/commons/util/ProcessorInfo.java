package org.infinispan.commons.util;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Provides general information about the processors on this host.
 *
 * @author Tristan Tarrant
 */
public class ProcessorInfo {

   private ProcessorInfo() {
   }

   /**
    * Returns the number of processors available to this process. On most operating systems this method
    * simply delegates to {@link Runtime#availableProcessors()}. However, before Java 10, under Linux this strategy
    * is insufficient, since the JVM does not take into consideration the process' CPU set affinity and the CGroups
    * quota/period assignment. Therefore this method will analyze the Linux proc filesystem
    * to make the determination. Since the CPU affinity of a process can be change at any time, this method does
    * not cache the result. Calls should be limited accordingly.
    * <br>
    * The number of available processors can be overridden via the system property <tt>infinispan.activeprocessorcount</tt>,
    * e.g. <tt>java -Dinfinispan.activeprocessorcount=4 ...</tt>. Note that this value cannot exceed the actual number
    * of available processors.
    * <br>
    * Since Java 10, this can also be achieved via the VM flag <tt>-XX:ActiveProcessorCount=xx</tt>.
    * <br>
    * Note that on Linux, both SMT units (Hyper-Threading) and CPU cores are counted as a processor.
    *
    * @return the available processors on this system.
    */
   public static int availableProcessors() {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged((PrivilegedAction<Integer>) () ->
               Integer.valueOf(org.infinispan.commons.jdkspecific.ProcessorInfo.availableProcessors()).intValue());
      }

      return org.infinispan.commons.jdkspecific.ProcessorInfo.availableProcessors();
   }

   public static void main(String args[]) {
      System.out.println(availableProcessors());
   }
}
