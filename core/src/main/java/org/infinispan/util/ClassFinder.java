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
package org.infinispan.util;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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
   
   private static final Log log = LogFactory.getLog(ClassFinder.class); 
   
   public static final String PATH = System.getProperty("java.class.path") + File.pathSeparator
            + System.getProperty("surefire.test.class.path");

   public static List<Class<?>> withAnnotationPresent(List<Class<?>> classes, Class<? extends Annotation> c) {
      List<Class<?>> clazzes = new ArrayList<Class<?>>(classes.size());
      for (Class<?> clazz : classes) {
         if (clazz.isAnnotationPresent(c)) {
            clazzes.add(clazz);
         }
      }
      return clazzes;
   }
   
   public static List<Class<?>> withAnnotationDeclared(List<Class<?>> classes, Class<? extends Annotation> c) {
      List<Class<?>> clazzes = new ArrayList<Class<?>>(classes.size());
      for (Class<?> clazz : classes) {
         if (clazz.isAnnotationPresent(c)) {
            Annotation[] declaredAnnots = clazz.getDeclaredAnnotations();
            for (Annotation declaredAnnot : declaredAnnots) {
               if (declaredAnnot.annotationType().isAssignableFrom(c)) {
                  clazzes.add(clazz);
               }
            }
         }
      }
      return clazzes;
   }

   public static List<Class<?>> isAssignableFrom(List<Class<?>> classes, Class<?> clazz) {
      List<Class<?>> clazzes = new ArrayList<Class<?>>(classes.size());
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
         if (path.contains("infinispan")) {
            files.add(new File(path));
         }
      }
      log.debugf("Looking for infinispan classes in %s", files);
      if (files.isEmpty()) {
         return Collections.emptyList();
      } else {
         Set<Class<?>> classFiles = new HashSet<Class<?>>();
         for (File file : files) {
            classFiles.addAll(findClassesOnPath(file));
         }
         return new ArrayList<Class<?>>(classFiles);
      }
   }

   private static List<Class<?>> findClassesOnPath(File path) {
      List<Class<?>> classes = new ArrayList<Class<?>>();
      Class<?> claz;

      if (path.isDirectory()) {
         List<File> classFiles = new ArrayList<File>();
         dir(classFiles, path);
         for (File cf : classFiles) {
            String clazz = null;
            try {
               clazz = toClassName(cf.getAbsolutePath());
               claz = Util.loadClassStrict(clazz, null);
               classes.add(claz);
            } catch (NoClassDefFoundError ncdfe) {
               log.warnf("%s has reference to a class %s that could not be loaded from classpath",
                         cf.getAbsolutePath(), ncdfe.getMessage());
            } catch (Throwable e) {
               // Catch all since we do not want skip iteration
               log.warn("On path " + cf.getAbsolutePath() + " could not load class "+ clazz, e);
            }
         }
      } else {
         if (path.isFile() && path.getName().endsWith("jar") && path.canRead()) {
            JarFile jar;
            try {
               jar = new JarFile(path);
            } catch (Exception ex) {
               log.warnf("Could not create jar file on path %s", path);
               return classes;
            }
            Enumeration<JarEntry> en = jar.entries();
            while (en.hasMoreElements()) {
               JarEntry entry = en.nextElement();
               if (entry.getName().endsWith("class")) {
                  String clazz = null;
                  try {
                     clazz = toClassName(entry.getName());
                     claz = Util.loadClassStrict(clazz, null);
                     classes.add(claz);
                  } catch (NoClassDefFoundError ncdfe) {
                     log.warnf("%s has reference to a class %s that could not be loaded from classpath",
                               entry.getName(), ncdfe.getMessage());
                  } catch (Throwable e) {
                     // Catch all since we do not want skip iteration
                     log.warn("From jar path " + entry.getName() + " could not load class "+ clazz, e);
                  }
               }
            }
         }
      }
      return classes;
   }

   private static void dir(List<File> files, File dir) {
      File[] entries = dir.listFiles();
      if (entries != null) {
         for (File entry : entries) {
            if (entry.isDirectory()) {
               dir(files, entry);
            } else if (entry.getName().endsWith("class")) {
               files.add(entry);
            }
         }
      }
   }

   private static String toClassName(String fileName) {
      return fileName.substring(fileName.lastIndexOf("org" + File.separator), fileName.length() - 6).replace(File.separator, ".");
   }
}