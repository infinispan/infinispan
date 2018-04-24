package org.infinispan.doclets.jmx;

/**
 * An MBean attribute
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class MBeanAttribute extends JmxComponent {
   public boolean writable;
   public String type;

   @Override
   public String toString() {
      return "Attribute(name = " + name + ", writable = " + writable + ", type = " + type + ", desc = " + desc + ")";
   }
}
