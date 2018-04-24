package org.infinispan.doclets.jmx;

/**
 * An MBean operation
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class MBeanOperation extends JmxComponent {
   public String returnType = "void";
   public String signature = "";
   int n = 0;

   @Override
   public String toString() {
      return "Operation(name = " + name + ", desc = " + desc + ", sig = " + signature + ", retType = " + returnType + ")";
   }

   void addParam(String paramType) {
      if (signature.length() != 0) signature += ", ";
      signature += paramType + " p" + n++;
   }
}
