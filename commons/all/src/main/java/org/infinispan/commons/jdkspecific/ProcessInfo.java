package org.infinispan.commons.jdkspecific;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.List;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class ProcessInfo {

   private final String name;
   private final long pid;
   private final List<String> arguments;

   private ProcessInfo() {
      RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
      String[] nameParts = runtimeMxBean.getName().split("@");
      name = runtimeMxBean.getName();
      pid = Long.parseLong(nameParts[0]);
      arguments = runtimeMxBean.getInputArguments();
   }

   public static ProcessInfo getInstance() {
      return new ProcessInfo();
   }

   public String getName() {
      return name;
   }

   public long getPid() {
      return pid;
   }

   public List<String> getArguments() {
      return arguments;
   }

   public ProcessInfo getParent() {
      return null; // Java 8 cannot retrieve this
   }

   @Override
   public String toString() {
      return "Process{" +
            "name='" + name + '\'' +
            ", pid=" + pid +
            ", arguments=" + arguments +
            '}';
   }
}
