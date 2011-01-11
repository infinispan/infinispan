/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan;

import net.jcip.annotations.Immutable;

/**
 * Contains version information about this release of Infinispan.
 *
 * @author Bela Ban
 * @since 4.0
 */
@Immutable
public class Version {

   // -----------------------------------------------------------------------------------------------------------------
   //   Please make sure versions saved in this file ALWAYS end with '-SNAPSHOT'.  This is the only way the release
   //   scripts can properly detect and substitute the versions for the correct versions at release-time.  Be careful
   //   to note that this is '-SNAPSHOT' and not '.SNAPSHOT', as per Maven's versioning conventions. -- Manik Surtani
   // -----------------------------------------------------------------------------------------------------------------

   public static final String major = "4.2";
   public static final String version = major + ".1-SNAPSHOT";
   public static final String codename = "Ursus";
   public static final String projectName = "Infinispan";
   static final byte[] version_id = {'0', '4', '2', '1', 'S'};
   private static final int MAJOR_SHIFT = 11;
   private static final int MINOR_SHIFT = 6;
   private static final int MAJOR_MASK = 0x00f800;
   private static final int MINOR_MASK = 0x0007c0;
   private static final int PATCH_MASK = 0x00003f;

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
      System.out.println(projectName);
      System.out.println();
      System.out.println("\nVersion: \t" + version);
      System.out.println("Codename: \t" + codename);
      System.out.println("History:  \t(see https://jira.jboss.org/jira/browse/ISPN for details)\n");
   }

   /**
    * Returns version information as a string.
    */
   public static String printVersion() {
      return projectName + " '" + codename + "' " + version;
   }

   public static String printVersionId(byte[] v, int len) {
      StringBuilder sb = new StringBuilder();
      if (v != null) {
         if (len <= 0)
            len = v.length;
         for (int i = 0; i < len; i++)
            sb.append((char) v[i]);
      }
      return sb.toString();
   }

   public static String printVersionId(byte[] v) {
      StringBuilder sb = new StringBuilder();
      if (v != null) {
         for (byte aV : v) sb.append((char) aV);
      }
      return sb.toString();
   }


   public static boolean compareTo(byte[] v) {
      if (v == null)
         return false;
      if (v.length < version_id.length)
         return false;
      for (int i = 0; i < version_id.length; i++) {
         if (version_id[i] != v[i])
            return false;
      }
      return true;
   }

   public static int getLength() {
      return version_id.length;
   }

   public static short getVersionShort() {
      return getVersionShort(version);
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

   public static short encodeVersion(int major, int minor, int patch) {
      return (short) ((major << MAJOR_SHIFT)
            + (minor << MINOR_SHIFT)
            + patch);
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

   private static String[] getParts(String versionString) {
      return versionString.split("[\\.\\-]");
   }

   public static String getMajorVersion() {
      String[] parts = getParts(version);
      return parts[0] + "." + parts[1];
   }
}
