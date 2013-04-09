/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
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

import java.io.ByteArrayOutputStream;

/**
 * Contains version information about this release of Infinispan.
 *
 * @author Bela Ban
 * @since 4.0
 */
@Immutable
public class Version {

   private static final String MAJOR = "5";
   private static final String MINOR = "3";
   private static final String MICRO = "0";
   private static final String MODIFIER = "SNAPSHOT";
   private static final boolean SNAPSHOT = true;

   public static final String VERSION = String.format("%s.%s.%s%s%s", MAJOR, MINOR, MICRO, SNAPSHOT ? "-" : ".", MODIFIER);
   public static final String CODENAME = "Tactical Nuclear Penguin";
   public static final String PROJECT_NAME = "Infinispan";
   public static final byte[] VERSION_ID = readVersionBytes();
   public static final String MAJOR_MINOR = MAJOR + "." + MINOR;

   private static final int MAJOR_SHIFT = 11;
   private static final int MINOR_SHIFT = 6;
   private static final int MAJOR_MASK = 0x00f800;
   private static final int MINOR_MASK = 0x0007c0;
   private static final int PATCH_MASK = 0x00003f;

   private static byte[] readVersionBytes() {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      for (int i = 0; i < MAJOR.length(); i++) baos.write(MAJOR.charAt(i));
      for (int i = 0; i < MINOR.length(); i++) baos.write(MINOR.charAt(i));
      for (int i = 0; i < MICRO.length(); i++) baos.write(MICRO.charAt(i));
      if (SNAPSHOT)
         baos.write('S');
      else
         for (int i = 0; i < MODIFIER.length(); i++) baos.write(MODIFIER.charAt(i));
      return baos.toByteArray();
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
      System.out.printf("Version: \t%s%n", VERSION);
      System.out.printf("Codename: \t%s%n", CODENAME);
      System.out.println("History: \t(see https://jira.jboss.org/jira/browse/ISPN for details)");
      System.out.println();
   }

   /**
    * Returns version information as a string.
    */
   public static String printVersion() {
      return PROJECT_NAME + " '" + CODENAME + "' " + VERSION;
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

   public static boolean compareTo(byte[] v) {
      if (v == null)
         return false;
      if (v.length < VERSION_ID.length)
         return false;
      for (int i = 0; i < VERSION_ID.length; i++) {
         if (VERSION_ID[i] != v[i])
            return false;
      }
      return true;
   }

   public static int getLength() {
      return VERSION_ID.length;
   }

   public static short getVersionShort() {
      return getVersionShort(VERSION);
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
}
