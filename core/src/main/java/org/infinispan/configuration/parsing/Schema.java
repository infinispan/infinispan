package org.infinispan.configuration.parsing;

/**
 * Schema.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public class Schema {
   final int major;
   final int minor;

   public Schema(int major, int minor) {
      this.major = major;
      this.minor = minor;
   }

   public int getMajor() {
      return major;
   }

   public int getMinor() {
      return minor;
   }

   public boolean since(int major, int minor) {
      return (this.major > major) || ((this.major == major) && (this.minor >= minor));
   }

   public static Schema fromNamespaceURI(String namespaceURI) {
      if (namespaceURI.startsWith("uri:")) {
         String version = namespaceURI.substring(namespaceURI.lastIndexOf(':') + 1);
         String[] split = version.split("\\.");
         return new Schema(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
      } else {
         return new Schema(999, 999);
      }
   }

}
