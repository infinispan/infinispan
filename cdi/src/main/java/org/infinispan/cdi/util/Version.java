package org.infinispan.cdi.util;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class Version {
   /**
    * Returns the version of the CDI extension.
    *
    * @return the CDI extension version.
    */
   public static String getVersion() {
      return "[WORKING]";
   }

   /**
    * Main method used to display the extension version.
    */
   public static void main(String[] args) {
      System.out.println();
      System.out.println("Infinispan CDI extension");
      System.out.println("Version: " + getVersion());
   }
}
