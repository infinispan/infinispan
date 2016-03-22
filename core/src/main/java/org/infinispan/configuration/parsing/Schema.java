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
      int major = 999;
      int minor = 999;
      if (namespaceURI.startsWith("uri:") || namespaceURI.startsWith("urn:")) {
         String version = namespaceURI.substring(namespaceURI.lastIndexOf(':') + 1);
         String[] split = version.split("\\.");
         try {
            major = Integer.parseInt(split[0]);
            minor = Integer.parseInt(split[1]);
         } catch (NumberFormatException e) {
            // Ignore
         }
      }
      return new Schema(major, minor);
   }

}
