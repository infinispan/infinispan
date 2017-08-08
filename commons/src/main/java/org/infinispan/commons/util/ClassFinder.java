package org.infinispan.commons.util;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

/**
 * Find infinispan classes utility
 */
public class ClassFinder {

   private static final Log log = LogFactory.getLog(ClassFinder.class);

   public static final String PATH = SecurityActions.getProperty("java.class.path") + File.pathSeparator
            + SecurityActions.getProperty("surefire.test.class.path");

   public static List<Class<?>> withAnnotationPresent(List<Class<?>> classes, Class<? extends Annotation> c) {
      List<Class<?>> clazzes = new ArrayList<>(classes.size());
      for (Class<?> clazz : classes) {
         if (clazz.isAnnotationPresent(c)) {
            clazzes.add(clazz);
         }
      }
      return clazzes;
   }

   public static List<Class<?>> withAnnotationDeclared(List<Class<?>> classes, Class<? extends Annotation> c) {
      List<Class<?>> clazzes = new ArrayList<>(classes.size());
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
      List<Class<?>> clazzes = new ArrayList<>(classes.size());
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
      List<File> files = new ArrayList<>();

      // either infinispan jar or a directory of output classes contains infinispan classes
      for (String path : javaClassPath.split(File.pathSeparator)) {
         File file = new File(path);
         boolean isInfinispanJar = file.isFile() && file.getName().contains("infinispan");
         // Exclude the test utility classes in the commons-test module
         boolean isTargetDirectory = file.isDirectory() &&
               new File(file, "org/infinispan").isDirectory() &&
               !new File(file, "org/infinispan/commons/test").isDirectory();
         if (isInfinispanJar || isTargetDirectory) {
            files.add(file);
         }
      }
      log.debugf("Looking for infinispan classes in %s", files);
      if (files.isEmpty()) {
         return Collections.emptyList();
      } else {
         Set<Class<?>> classFiles = new HashSet<>();
         for (File file : files) {
            classFiles.addAll(findClassesOnPath(file));
         }
         return new ArrayList<>(classFiles);
      }
   }

   private static List<Class<?>> findClassesOnPath(File path) {
      List<Class<?>> classes = new ArrayList<>();
      Class<?> claz;

      if (path.isDirectory()) {
         List<File> classFiles = new ArrayList<>();
         dir(classFiles, path);
         for (File cf : classFiles) {
            String clazz = null;
            try {
               clazz = toClassName(path.toPath().relativize(cf.toPath()).toString());
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
            try {
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
            finally {
               try {
                  jar.close();
               } catch (IOException e) {
                  log.debugf(e, "error closing jar file %s", jar);
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

   private static String toClassName(String classFileName) {
      // Remove the .class suffix and replace / with .
      return classFileName.substring(0, classFileName.length() - 6)
                          .replace(File.separator, ".");
   }
}
