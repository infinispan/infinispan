package org.infinispan.commons.util;

/**
 * Operating system family.
 *
 * Technically Solaris is UNIX, but for test purposes we are classifying it as a separate family.
 *
 * @since 9.2
 */
public enum OS {
   UNIX, WINDOWS, SOLARIS, LINUX, MAC_OS;

   public static OS getCurrentOs() {
      String os = System.getProperty("os.name").toLowerCase();
      if (os.contains("win")) {
         return WINDOWS;
      } else if (os.contains("sunos")) {
         return SOLARIS;
      } else if (os.contains("linux")) {
         return LINUX;
      } else if (os.toLowerCase().contains("mac os")) {
         return MAC_OS;
      } else {
         return UNIX;
      }
   }
}
