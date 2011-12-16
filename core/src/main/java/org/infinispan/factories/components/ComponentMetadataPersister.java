/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.factories.components;

import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.ScopeDetector;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.util.ReflectionUtil;
import org.infinispan.util.Util;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A utility class used by the Infinispan build process to scan metadata and persist it.  Should be used by all
 * Infinispan modules that define components decorated with {@link Inject}, {@link Start}, {@link Stop}, etc.
 *
 * @author Manik Surtani
 * @see ComponentMetadataRepo
 * @see ComponentMetadata
 * @since 5.1
 */
public class ComponentMetadataPersister extends ComponentMetadataRepo {

   /**
    * Usage: ComponentMetadataPersister [path containing .class files to scan] [output file to generate]
    */
   public static void main(String[] args) throws ClassNotFoundException, IOException {
      // When run off the command-line or a build script, this program takes in two arguments: the path containing
      // class files to scan, and the output file to generate.
      long startTime = System.nanoTime();
      String path = args[0];
      String outputFile = args[1];

      System.out.printf(" [ComponentMetadataPersister] Starting component metadata generation.  Scanning classes in %s%n", path);

      // Wipe any stale data from memory first
      FACTORIES.clear();
      COMPONENT_METADATA_MAP.clear();

      File f = new File(path);
      process(path, f);

      // Test that all dependencies now exist in the component metadata map.
      Map<String, String> dependencies = new HashMap<String, String>(128);
      for (ComponentMetadata md : COMPONENT_METADATA_MAP.values()) {
         if (md.getDependencies() != null) dependencies.putAll(md.getDependencies());
      }

      ClassLoader cl = ComponentMetadataRepo.class.getClassLoader();
      for (String s : dependencies.keySet()) {
         if (!COMPONENT_METADATA_MAP.containsKey(s)) {
            // See if anything we already have is assignable from here.
            try {
               Class<?> dependencyType = Util.loadClass(s, cl);
               ComponentMetadata equivalent = null;
               for (ComponentMetadata cm : COMPONENT_METADATA_MAP.values()) {
                  if (dependencyType.isAssignableFrom(cm.getClazz())) {
                     equivalent = cm;
                     break;
                  }
               }
               if (equivalent != null) COMPONENT_METADATA_MAP.put(s, equivalent);
            } catch (Exception e) {
            }
         }
      }
      if (Boolean.getBoolean("infinispan.isCoreModule")) {
         // Perform this sanity check
         boolean hasErrors = false;
         for (Map.Entry<String, String> e : dependencies.entrySet()) {
            if (!COMPONENT_METADATA_MAP.containsKey(e.getKey())) {
               if (!hasFactory(e.getKey()) && !hasFactory(e.getValue()) && !KnownComponentNames.ALL_KNOWN_COMPONENT_NAMES.contains(e.getKey())) {
                  System.out.printf(" [ComponentMetadataPersister]     **** WARNING!!!  Missing components or factories for dependency on %s%n", e.getKey());
                  hasErrors = true;
               }
            }
         }

         if (hasErrors && Boolean.getBoolean("infinispan.isCoreModule"))
            throw new RuntimeException("Could not pass sanity check of all annotated components and their respective factories/dependencies.");
      }
      writeMetadata(outputFile);

      System.out.printf(" [ComponentMetadataPersister] %s components and %s factories analyzed and persisted in %s.%n%n", COMPONENT_METADATA_MAP.size(), FACTORIES.size(), Util.prettyPrintTime(System.nanoTime() - startTime, TimeUnit.NANOSECONDS));
   }

   private static boolean hasFactory(String name) {
      return FACTORIES.containsKey(name);
   }

   private static void process(String path, File f) throws ClassNotFoundException {
      if (f.isDirectory()) {
         for (File child : f.listFiles()) process(path, child);
      } else if (isValidClassFile(f)) {
         // Process this class file.
         String fqcn = extractFqcn(path, f);
         processClass(ComponentMetadataRepo.class.getClassLoader().loadClass(fqcn), fqcn);
      }
   }


   private static boolean isValidClassFile(File f) {
      // Valid classes end with .class
      return f.getName().endsWith(".class");
   }

   private static void processClass(Class<?> clazz, String className) {
      // Look for a @MBean annotation first.
      MBean mbean = ReflectionUtil.getAnnotation(clazz, MBean.class);
      boolean survivesRestarts;
      boolean isGlobal;
      // Could still be a valid component.
      isGlobal = ScopeDetector.detectScope(clazz) == Scopes.GLOBAL;
      survivesRestarts = ReflectionUtil.getAnnotation(clazz, SurvivesRestarts.class) != null;

      List<Method> injectMethods = ReflectionUtil.getAllMethods(clazz, Inject.class);
      List<Method> startMethods = ReflectionUtil.getAllMethods(clazz, Start.class);
      List<Method> stopMethods = ReflectionUtil.getAllMethods(clazz, Stop.class);

      ComponentMetadata metadata = null;

      if (mbean != null) {
         List<Method> managedAttributeMethods = ReflectionUtil.getAllMethods(clazz, ManagedAttribute.class);
         List<Field> managedAttributeFields = ReflectionUtil.getAnnotatedFields(clazz, ManagedAttribute.class);
         List<Method> managedOperationMethods = ReflectionUtil.getAllMethods(clazz, ManagedOperation.class);
         metadata = new ManageableComponentMetadata(clazz, injectMethods, startMethods, stopMethods, isGlobal,
                                                    survivesRestarts, managedAttributeFields, managedAttributeMethods,
                                                    managedOperationMethods, mbean);
      } else if (!injectMethods.isEmpty() || !startMethods.isEmpty() || !stopMethods.isEmpty()
            || isGlobal || survivesRestarts || ReflectionUtil.isAnnotationPresent(clazz, Scope.class)) {
         // Then this still is a component!
         metadata = new ComponentMetadata(clazz, injectMethods, startMethods, stopMethods, isGlobal, survivesRestarts);
      }

      if (metadata != null) {
         COMPONENT_METADATA_MAP.put(metadata.getName(), metadata);
      }

      // and also lets check if this class is a factory for anything.
      DefaultFactoryFor dff = ReflectionUtil.getAnnotation(clazz, DefaultFactoryFor.class);

      if (dff != null) {
         for (Class target : dff.classes()) FACTORIES.put(target.getName(), className);
      }
   }

   private static String extractFqcn(String path, File f) {
      return f.getAbsolutePath().replace(path, "").replace(File.separator, ".").replace(".class", "").replaceFirst(".", "");
   }

   private static void writeMetadata(String metadataFile) throws IOException {
      FileOutputStream fileOutputStream = new FileOutputStream(metadataFile);
      BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
      ObjectOutputStream objectOutputStream = new ObjectOutputStream(bufferedOutputStream);
      objectOutputStream.writeObject(COMPONENT_METADATA_MAP);
      objectOutputStream.writeObject(FACTORIES);
      objectOutputStream.flush();
      objectOutputStream.close();
      bufferedOutputStream.flush();
      bufferedOutputStream.close();
      fileOutputStream.flush();
      fileOutputStream.close();
      System.out.printf(" [ComponentMetadataPersister] Persisted metadata in %s%n", metadataFile);
   }
}
