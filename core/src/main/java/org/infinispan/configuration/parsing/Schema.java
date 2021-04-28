package org.infinispan.configuration.parsing;

import org.infinispan.commons.configuration.io.ConfigurationSchemaVersion;
import org.infinispan.commons.util.Version;

/**
 * Schema.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public class Schema implements ConfigurationSchemaVersion {
   private final String uri;
   private final int major;
   private final int minor;

   public Schema(String uri, int major, int minor) {
      this.uri = uri;
      this.major = major;
      this.minor = minor;
   }

   @Override
   public String getURI() {
      return uri;
   }

   @Override
   public int getMajor() {
      return major;
   }

   @Override
   public int getMinor() {
      return minor;
   }

   @Override
   public boolean since(int major, int minor) {
      return (this.major > major) || ((this.major == major) && (this.minor >= minor));
   }

   public static Schema fromNamespaceURI(String namespaceURI) {
      int major = Integer.parseInt(Version.getMajor());
      int minor = Integer.parseInt(Version.getMinor());
      if (namespaceURI.startsWith("uri:") || namespaceURI.startsWith("urn:")) {
         int colon = namespaceURI.lastIndexOf(':');
         String uri = namespaceURI.substring(0, colon);
         String version = namespaceURI.substring(colon + 1);
         String[] split = version.split("\\.");
         try {
            major = Integer.parseInt(split[0]);
            minor = Integer.parseInt(split[1]);
         } catch (NumberFormatException e) {
            // Ignore
         }
         return new Schema(uri, major, minor);
      } else {
         return new Schema("", major, minor);
      }
   }

}
