package org.infinispan.doclets.jmx;

import java.util.LinkedList;
import java.util.List;

/**
 * * An MBean component
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class MBeanComponent extends JmxComponent {
   public String className;
   public List<MBeanOperation> operations = new LinkedList<MBeanOperation>();
   public List<MBeanAttribute> attributes = new LinkedList<MBeanAttribute>();

   @Override
   public String toString() {
      return "MBean component " + name + " (class " + className + ") op = " + operations + " attr = " + attributes;
   }
}
