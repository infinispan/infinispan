package org.infinispan;

import net.jcip.annotations.Immutable;

/**
 * Contains version information about this release of Infinispan.
 *
 * @author Bela Ban
 * @since 4.0
 * @deprecated Use {@link org.infinispan.commons.util.Version} instead
 */
@Immutable
@Deprecated
public class Version {

   public static String getVersion() {
      return org.infinispan.commons.util.Version.getVersion();
   }

   public static String getBrandName() {
      return org.infinispan.commons.util.Version.getBrandName();
   }

   public static String getCodename() {
      return org.infinispan.commons.util.Version.getCodename();
   }

   public static String getModuleSlot() {
      return org.infinispan.commons.util.Version.getModuleSlot();
   }

   public static short getMarshallVersion() {
      return org.infinispan.commons.util.Version.getMarshallVersion();
   }

   public static String getSchemaVersion() {
      return org.infinispan.commons.util.Version.getSchemaVersion();
   }

   public static String getMajorMinor() {
      return org.infinispan.commons.util.Version.getMajorMinor();
   }

   public static String getMajor() {
      return org.infinispan.commons.util.Version.getMajor();
   }

   public static boolean compareTo(byte[] v) {
      return org.infinispan.commons.util.Version.compareTo(v);
   }

   public static short getVersionShort() {
      return org.infinispan.commons.util.Version.getVersionShort();
   }

   public static short getVersionShort(String versionString) {
      return org.infinispan.commons.util.Version.getVersionShort(versionString);
   }

   public static String decodeVersion(short version) {
      return org.infinispan.commons.util.Version.decodeVersion(version);
   }

   /**
    * Serialization only looks at major and minor, not micro or below.
    */
   public static String decodeVersionForSerialization(short version) {
      return org.infinispan.commons.util.Version.decodeVersionForSerialization(version);
   }

   /**
    * Prints version information.
    */
   public static void main(String[] args) {
      org.infinispan.commons.util.Version.main(args);
   }

   /**
    * Prints full version information to the standard output.
    */
   public static void printFullVersionInformation() {
      org.infinispan.commons.util.Version.printFullVersionInformation();
   }

   /**
    * Returns version information as a string.
    */
   public static String printVersion() {
      return org.infinispan.commons.util.Version.printVersion();
   }
}
