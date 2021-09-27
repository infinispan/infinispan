package org.infinispan.commons.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

import net.jcip.annotations.Immutable;

/**
 * Contains version information about this release of Infinispan.
 *
 * @author Bela Ban
 * @since 4.0
 */
@Immutable
public class Version {
   private static final int MAJOR_SHIFT = 11;
   private static final int MINOR_SHIFT = 6;
   private static final int MAJOR_MASK = 0x00f800;
   private static final int MINOR_MASK = 0x0007c0;
   private static final int PATCH_MASK = 0x00003f;

   private static final Version INSTANCE = new Version();
   public static final String INFINISPAN_VERSION = "infinispan.version";
   public static final String INFINISPAN_BRAND_NAME = "infinispan.brand.name";
   public static final String INFINISPAN_BRAND_VERSION = "infinispan.brand.version";
   public static final String INFINISPAN_CODENAME = "infinispan.codename";
   public static final String INFINISPAN_CORE_SCHEMA_VERSION = "infinispan.core.schema.version";
   public static final String INFINISPAN_MODULE_SLOT_PREFIX = "infinispan.module.slot.prefix";
   public static final String INFINISPAN_MODULE_SLOT_VERSION = "infinispan.module.slot.version";

   private final String version;
   private final String brandName;
   private final String brandVersion;
   private final String codename;
   private final String schemaVersion;
   private final byte[] versionId;
   private final String moduleSlot;
   private final short versionShort;
   private final short marshallVersion;
   private final String majorMinor;
   private final String major;
   private final String minor;
   private final Properties properties;

   private Version() {
      this(Version.class.getResourceAsStream("/META-INF/infinispan-version.properties"));
   }

   private Version(InputStream is) {
      properties = new Properties();
      try {
         properties.load(is);
         // Closing it here is harmless
         Util.close(is);
      } catch (IOException e) {
         // Ignore errors, we'll use fallbacks
      }
      version = properties.getProperty(INFINISPAN_VERSION, "0.0.0-SNAPSHOT");
      brandName = properties.getProperty(INFINISPAN_BRAND_NAME, "Infinispan");
      brandVersion = properties.getProperty(INFINISPAN_BRAND_VERSION, version);
      codename = properties.getProperty(INFINISPAN_CODENAME, "N/A");
      schemaVersion = properties.getProperty(INFINISPAN_CORE_SCHEMA_VERSION, "0.0");
      String parts[] = getParts(version);
      versionId = readVersionBytes(parts[0], parts[1], parts[2], parts[3]);
      versionShort = getVersionShort(version);
      String modulePrefix = properties.getProperty(INFINISPAN_MODULE_SLOT_PREFIX, "ispn");
      String moduleVersion = properties.getProperty(INFINISPAN_MODULE_SLOT_VERSION, parts[0] + "." + parts[1]);
      moduleSlot = String.format("%s-%s", modulePrefix, moduleVersion);
      marshallVersion = Short.valueOf(parts[0] + parts[1]);
      majorMinor = String.format("%s.%s", parts[0], parts[1]);
      major = parts[0];
      minor = parts[1];
   }

   public static Version from(InputStream is) {
      return new Version(is);
   }

   /*
    * The following methods are per-instance
    */
   public String version() {
      return version;
   }

   public String brandName() {
      return brandName;
   }

   public String brandVersion() {
      return brandVersion;
   }

   /*
    * The following methods use a singleton instance
    */
   public static String getVersion() {
      return INSTANCE.version;
   }

   public static String getBrandName() {
      return INSTANCE.brandName;
   }

   public static String getBrandVersion() {
      return INSTANCE.brandVersion;
   }

   public static String getCodename() {
      return INSTANCE.codename;
   }

   public static String getModuleSlot() {
      return INSTANCE.moduleSlot;
   }

   public static short getMarshallVersion() {
      return INSTANCE.marshallVersion;
   }

   public static String getSchemaVersion() {
      return INSTANCE.schemaVersion;
   }

   public static String getMajorMinor() {
      return INSTANCE.majorMinor;
   }

   public static String getMajor() {
      return INSTANCE.major;
   }

   public static String getMinor() {
      return INSTANCE.minor;
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

   public static String getProperty(String name) {
      return INSTANCE.properties.getProperty(name);
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
      System.out.println(INSTANCE.brandName);
      System.out.println();
      System.out.printf("Version: \t%s%n", INSTANCE.brandVersion);
      System.out.printf("Codename: \t%s%n", INSTANCE.codename);
      System.out.println();
   }

   /**
    * Returns version information as a string.
    */
   public static String printVersion() {
      return INSTANCE.brandName + " '" + INSTANCE.codename + "' " + INSTANCE.brandVersion;
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
}
