/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
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
package org.infinispan.util;

import java.io.File;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Find infinispan classes utility
 */
public class ClassFinder {
   
   public static String PATH = System.getProperty("java.class.path") + File.pathSeparator
            + System.getProperty("surefire.test.class.path");

   public static List<Class<?>> withAnnotationPresent(List<Class<?>> classes, Class<? extends Annotation> c) {
      List<Class<?>> clazzes = new ArrayList<Class<?>>();
      for (Class<?> clazz : classes) {
         if (clazz.isAnnotationPresent(c)) {
            clazzes.add(clazz);
         }
      }
      return clazzes;
   }

   public static List<Class<?>> isAssignableFrom(List<Class<?>> classes, Class<?> clazz) {
      List<Class<?>> clazzes = new ArrayList<Class<?>>();
      for (Class<?> c : classes) {
         if (clazz.isAssignableFrom(c)) {
            clazzes.add(c);
         }
      }
      return clazzes;
   }

   public static List<Class<?>> withAnnotationPresent(Class<? extends Annotation> ann) throws Exception {
      return withAnnotationPresent(infinispanClasses(), ann);
   }

   public static List<Class<?>> isAssignableFrom(Class<?> clazz) throws Exception {
      return isAssignableFrom(infinispanClasses(), clazz);
   }

   public static List<Class<?>> infinispanClasses() throws Exception {
      return infinispanClasses(PATH);
   }

   public static List<Class<?>> infinispanClasses(String javaClassPath) throws Exception {
      List<File> files = new ArrayList<File>();

      // either infinispan jar or a directory of output classes contains infinispan classes
      for (String path : javaClassPath.split(File.pathSeparator)) {
         if ((path.contains("infinispan") && path.endsWith("jar")) || !path.endsWith("jar")) {
            files.add(new File(path));
         }
      }

      if (files.isEmpty())
         return Collections.emptyList();
      else if (files.size() == 1)
         return findClassesOnPath(files.get(0));
      else {
         Set<Class<?>> classFiles = new HashSet<Class<?>>();
         for (File file : files) {
            classFiles.addAll(findClassesOnPath(file));
         }
         return new ArrayList<Class<?>>(classFiles);
      }
   }

   private static List<Class<?>> findClassesOnPath(File path) throws Exception {
      List<Class<?>> classes = new ArrayList<Class<?>>();
      try {
         if (path.isDirectory()) {
            List<File> classFiles = new ArrayList<File>();
            dir(classFiles, path);
            for (File cf : classFiles) {
               Class<?> claz = Util.loadClass(toClassName(cf.getAbsolutePath().toString()));
               classes.add(claz);
            }
         } else {
            if (path.isFile() && path.getName().endsWith("jar") && path.canRead()) {
               JarFile jar = new JarFile(path);
               Enumeration<JarEntry> en = jar.entries();
               while (en.hasMoreElements()) {
                  JarEntry entry = en.nextElement();
                  if (entry.getName().endsWith("class")) {
                     Class<?> claz = Util.loadClass(toClassName(entry.getName()));
                     classes.add(claz);                      
                  }
               }
            }
         }
      } catch (NoClassDefFoundError e) {
         // unable to load these classes!!
         e.printStackTrace();
      } catch (ClassNotFoundException e) {
         // unable to load these classes!!
         e.printStackTrace();
      }
      return classes;
   }

   private static void dir(List<File> files, File dir) {
      File[] entries = dir.listFiles();
      for (File entry : entries) {
         if (entry.isDirectory()) {
            dir(files, entry);
         } else if (entry.getName().endsWith("class")) {
            files.add(entry);
         }
      }
   }

   private static String toClassName(String fileName) {
      return fileName.substring(fileName.lastIndexOf("org"), fileName.length() - 6).replaceAll(File.separator, ".");
   }
}