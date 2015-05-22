package org.infinispan;

import net.jcip.annotations.Immutable;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

/**
 * Contains version information about this release of Infinispan.
 *
 * @author Bela Ban
 * @since 4.0
 */
@Immutable
public class Version {
   private static final String MODULE_PREFIX = "ispn";
   private static final int MAJOR_SHIFT = 11;
   private static final int MINOR_SHIFT = 6;
   private static final int MAJOR_MASK = 0x00f800;
   private static final int MINOR_MASK = 0x0007c0;
   private static final int PATCH_MASK = 0x00003f;

   public static final String PROJECT_NAME = "Infinispan";

   private static final Version INSTANCE = new Version();

   private final byte[] versionId;
   private final String moduleSlot;
   private final short versionShort;
   private final short marshallVersion;
   private final String majorMinor;

   private Version() {
      String parts[] = getParts(Injected.getVersion());
      versionId = readVersionBytes(parts[0], parts[1], parts[2], parts[3]);
      versionShort = getVersionShort(Injected.getVersion());
      moduleSlot = String.format("%s-%s.%s", MODULE_PREFIX, parts[0], parts[1]);
      marshallVersion = Short.valueOf(parts[0] + parts[1]);
      majorMinor = String.format("%s.%s", parts[0], parts[1]);
   }

   public static String getVersion() {
      return Injected.getVersion();
   }

   public static String getModuleSlot() {
      return INSTANCE.moduleSlot;
   }

   public static short getMarshallVersion() {
      return INSTANCE.marshallVersion;
   }

   public static String getMajorMinor() {
      return INSTANCE.majorMinor;
   }

   public static boolean compareTo(byte[] v) {
      return Arrays.equals(INSTANCE.versionId, v);
   }

   public static short getVersionShort() {
      return INSTANCE.versionShort;
   }

   public static short getVersionShort(String versionString) {
      if (versionString == null)
         throw new IllegalArgumentException("versionString is null");

      String parts[] = getParts(versionString);
      int a = 0;
      int b = 0;
      int c = 0;
      if (parts.length > 0)
         a = Integer.parseInt(parts[0]);
      if (parts.length > 1)
         b = Integer.parseInt(parts[1]);
      if (parts.length > 2)
         c = Integer.parseInt(parts[2]);
      return encodeVersion(a, b, c);
   }

   private static short encodeVersion(int major, int minor, int patch) {
      return (short) ((major << MAJOR_SHIFT) + (minor << MINOR_SHIFT) + patch);
   }

   public static String decodeVersion(short version) {
      int major = (version & MAJOR_MASK) >> MAJOR_SHIFT;
      int minor = (version & MINOR_MASK) >> MINOR_SHIFT;
      int patch = (version & PATCH_MASK);
      return major + "." + minor + "." + patch;
   }

   /**
    * Serialization only looks at major and minor, not micro or below.
    */
   public static String decodeVersionForSerialization(short version) {
      int major = (version & MAJOR_MASK) >> MAJOR_SHIFT;
      int minor = (version & MINOR_MASK) >> MINOR_SHIFT;
      return major + "." + minor;
   }

   /**
    * Prints version information.
    */
   public static void main(String[] args) {
      printFullVersionInformation();
   }

   /**
    * Prints full version information to the standard output.
    */
   public static void printFullVersionInformation() {
      System.out.println(PROJECT_NAME);
      System.out.println();
      System.out.printf("Version: \t%s%n", Injected.getVersion());
      System.out.printf("Codename: \t%s%n", Injected.getCodename());
      System.out.println("History: \t(see https://jira.jboss.org/jira/browse/ISPN for details)");
      System.out.println();
   }

   /**
    * Returns version information as a string.
    */
   public static String printVersion() {
      return PROJECT_NAME + " '" + Injected.getCodename() + "' " + Injected.getVersion();
   }

   private static byte[] readVersionBytes(String major, String minor, String micro, String modifier) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      for (int i = 0; i < major.length(); i++)
         baos.write(major.charAt(i));
      for (int i = 0; i < minor.length(); i++)
         baos.write(minor.charAt(i));
      for (int i = 0; i < micro.length(); i++)
         baos.write(micro.charAt(i));
      if ("SNAPSHOT".equals(modifier))
         baos.write('S');
      else
         for (int i = 0; i < modifier.length(); i++)
            baos.write(modifier.charAt(i));
      return baos.toByteArray();
   }

   private static String[] getParts(String version) {
      return version.split("[\\.\\-]");
   }

   static class Injected {
      static String getVersion() {
         return "0.0.0-SNAPSHOT"; // Will be replaced by the Maven Injection plugin
      }
      
      static String getCodename() {
         return ""; // Will be replaced by the Maven Injection plugin
      }
   }
}

