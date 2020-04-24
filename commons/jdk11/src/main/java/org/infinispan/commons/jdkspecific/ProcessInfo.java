package org.infinispan.commons.jdkspecific;

import java.util.Arrays;
import java.util.List;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class ProcessInfo {
   private final String name;
   private final long pid;
   private final long ppid;
   private final List<String> arguments;

   private ProcessInfo(ProcessHandle handle) {
      name = handle.info().command().orElse("-");
      pid = handle.pid();
      ppid = handle.parent().map(ProcessHandle::pid).orElse(-1l);
      arguments = Arrays.asList(handle.info().arguments().orElse(new String[]{}));
   }

   public static ProcessInfo getInstance() {
      return new ProcessInfo(ProcessHandle.current());
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
      if (ppid > 0) {
         return new ProcessInfo(ProcessHandle.of(ppid).get());
      } else {
         return null;
      }
   }

   @Override
   public String toString() {
      return "Process{" +
            "name='" + name + '\'' +
            ", pid=" + pid +
            ", ppid=" + ppid +
            ", arguments=" + arguments +
            '}';
   }
}
