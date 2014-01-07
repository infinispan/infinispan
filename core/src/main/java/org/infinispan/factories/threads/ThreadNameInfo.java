package org.infinispan.factories.threads;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * @author Galder Zamarreño
 */
public class ThreadNameInfo {

   private final long globalThreadSequenceNum;
   private final long perFactoryThreadSequenceNum;
   private final long factorySequenceNum;
   private final String node;
   private final String component;

   ThreadNameInfo(long globalThreadSequenceNum, long perFactoryThreadSequenceNum,
         long factorySequenceNum, String node, String component) {
      this.globalThreadSequenceNum = globalThreadSequenceNum;
      this.perFactoryThreadSequenceNum = perFactoryThreadSequenceNum;
      this.factorySequenceNum = factorySequenceNum;
      this.node = node;
      this.component = component;
   }

   private static final Pattern searchPattern = Pattern.compile("([^%]+)|%.");

   /**
    * Format the thread name string.
    * <ul>
    * <li>{@code %%} - emit a percent sign</li>
    * <li>{@code %t} - emit the per-factory thread sequence number</li>
    * <li>{@code %g} - emit the global thread sequence number</li>
    * <li>{@code %f} - emit the factory sequence number</li>
    * <li>{@code %p} - emit the {@code ":"}-separated thread group path</li>
    * <li>{@code %i} - emit the thread ID</li>
    * <li>{@code %G} - emit the thread group name</li>
    * <li>{@code %n} - emit the node name</li>
    * <li>{@code %c} - emit the component name</li>
    * </ul>
    *
    * @param thread the thread
    * @param formatString the format string
    * @return the thread name string
    */
   public String format(Thread thread, String formatString) {
      final StringBuilder builder = new StringBuilder(formatString.length() * 5);
      final ThreadGroup group = thread.getThreadGroup();
      final Matcher matcher = searchPattern.matcher(formatString);
      while (matcher.find()) {
         if (matcher.group(1) != null) {
            builder.append(matcher.group());
         } else {
            switch (matcher.group().charAt(1)) {
               case '%': builder.append('%'); break;
               case 't': builder.append(perFactoryThreadSequenceNum); break;
               case 'g': builder.append(globalThreadSequenceNum); break;
               case 'f': builder.append(factorySequenceNum); break;
               case 'p': if (group != null) appendGroupPath(group, builder); break;
               case 'i': builder.append(thread.getId()); break;
               case 'G': if (group != null) builder.append(group.getName()); break;
               case 'n': if (node != null) builder.append(node); break;
               case 'c': if (component != null) builder.append(component); break;
            }
         }
      }
      return builder.toString();
   }

   private static void appendGroupPath(ThreadGroup group, StringBuilder builder) {
      final ThreadGroup parent = group.getParent();
      if (parent != null) {
         appendGroupPath(parent, builder);
         builder.append(':');
      }
      builder.append(group.getName());
   }

}
