package org.infinispan.factories.components;

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

import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.PostStart;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.ScopeDetector;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;

/**
 * A utility class used by the Infinispan build process to scan metadata and persist it.  Should be used by all
 * Infinispan modules that define components decorated with {@link Inject}, {@link Start}, {@link Stop}, etc.
 *
 * @author Manik Surtani
 * @see ComponentMetadataRepo
 * @see ComponentMetadata
 * @since 5.1
 */
public class ComponentMetadataPersister {
   /**
    * Usage: ComponentMetadataPersister [path containing .class files to scan] [output file to generate]
    */
   public static void main(String[] args) throws ClassNotFoundException, IOException {
      // When run off the command-line or a build script, this program takes in two arguments: the path containing
      // class files to scan, and the output file to generate.
      String path = args[0];
      String outputFile = args[1];

      System.out.printf(" [ComponentMetadataPersister] Starting component metadata generation.  Scanning classes in %s%n", path);

      ComponentMetadataRepo repo = new ComponentMetadataRepo();

      File f = new File(path);
      process(repo, path, f);

      // Test that all dependencies now exist in the component metadata map.
      Map<String, String> dependencies = new HashMap<>(128);
      for (ComponentMetadata md : repo.componentMetadataMap.values()) {
         if (md.getDependencies() != null) dependencies.putAll(md.getDependencies());
      }

      ClassLoader cl = ComponentMetadataRepo.class.getClassLoader();
      for (String s : dependencies.keySet()) {
         if (!repo.componentMetadataMap.containsKey(s)) {
            // See if anything we already have is assignable from here.
            try {
               Class<?> dependencyType = Util.loadClass(s, cl);
               ComponentMetadata equivalent = null;
               for (ComponentMetadata cm : repo.componentMetadataMap.values()) {
                  if (dependencyType.isAssignableFrom(cm.getClazz())) {
                     equivalent = cm;
                     break;
                  }
               }
               if (equivalent != null) repo.componentMetadataMap.put(s, equivalent);
            } catch (Exception e) {
            }
         }
      }
      if (Boolean.getBoolean("infinispan.isCoreModule")) {
         // Perform this sanity check
         boolean hasErrors = false;
         for (Map.Entry<String, String> e : dependencies.entrySet()) {
            if (!repo.componentMetadataMap.containsKey(e.getKey())) {
               if (!repo.hasFactory(e.getKey()) && !repo.hasFactory(e.getValue()) && !KnownComponentNames.ALL_KNOWN_COMPONENT_NAMES.contains(e.getKey())) {
                  System.out.printf(" [ComponentMetadataPersister]     **** WARNING!!!  Missing components or factories for dependency on %s%n", e.getKey());
                  hasErrors = true;
               }
            }
         }

         if (hasErrors)
            throw new RuntimeException("Could not pass sanity check of all annotated components and their respective factories/dependencies.");
      }
      writeMetadata(repo, outputFile);

      System.out.printf(" [ComponentMetadataPersister] %s components and %s factories.%n%n",
            repo.componentMetadataMap.size(), repo.factories.size());
   }

   private static void process(ComponentMetadataRepo repo, String path, File f) throws ClassNotFoundException {
      if (f.isDirectory()) {
         for (File child : f.listFiles()) process(repo, path, child);
      } else if (isValidClassFile(f)) {
         // Process this class file.
         String fqcn = extractFqcn(path, f);
         Class<?> clazz = ComponentMetadataRepo.class.getClassLoader().loadClass(fqcn);
         processClass(repo, clazz, fqcn);
      }
   }

   private static boolean isValidClassFile(File f) {
      // Valid classes end with .class
      return f.getName().endsWith(".class");
   }

   private static void processClass(ComponentMetadataRepo repo, Class<?> clazz, String className) {
      MBean mbean = ReflectionUtil.getAnnotation(clazz, MBean.class);

      // Could still be a valid component.
      boolean isGlobal = ScopeDetector.detectScope(clazz) == Scopes.GLOBAL;
      boolean survivesRestarts = ReflectionUtil.getAnnotation(clazz, SurvivesRestarts.class) != null;

      List<Method> injectMethods = ReflectionUtil.getAllMethods(clazz, Inject.class);
      List<Method> startMethods = ReflectionUtil.getAllMethods(clazz, Start.class);
      List<Method> postStartMethods = ReflectionUtil.getAllMethods(clazz, PostStart.class);
      List<Method> stopMethods = ReflectionUtil.getAllMethods(clazz, Stop.class);

      ComponentMetadata metadata = null;

      if (mbean != null) {
         List<Method> managedAttributeMethods = ReflectionUtil.getAllMethods(clazz, ManagedAttribute.class);
         List<Field> managedAttributeFields = ReflectionUtil.getAnnotatedFields(clazz, ManagedAttribute.class);
         List<Method> managedOperationMethods = ReflectionUtil.getAllMethods(clazz, ManagedOperation.class);
         metadata = new ManageableComponentMetadata(clazz, injectMethods, startMethods, postStartMethods, stopMethods, isGlobal,
                                                    survivesRestarts, managedAttributeFields, managedAttributeMethods,
                                                    managedOperationMethods, mbean);
      } else if (!injectMethods.isEmpty() || !startMethods.isEmpty() || !stopMethods.isEmpty()
            || isGlobal || survivesRestarts || ReflectionUtil.isAnnotationPresent(clazz, Scope.class)) {
         // Then this still is a component!
         metadata = new ComponentMetadata(clazz, injectMethods, startMethods, postStartMethods, stopMethods, isGlobal, survivesRestarts);
      }

      if (metadata != null) {
         repo.componentMetadataMap.put(metadata.getName(), metadata);
      }

      // and also lets check if this class is a factory for anything.
      DefaultFactoryFor dff = ReflectionUtil.getAnnotation(clazz, DefaultFactoryFor.class);

      if (dff != null) {
         for (Class<?> target : dff.classes()) repo.factories.put(target.getName(), className);
      }
   }

   private static String extractFqcn(String path, File f) {
      return f.getAbsolutePath().replace(path, "").replace(File.separator, ".").replaceAll("\\.class$", "").replaceFirst("\\.+", "");
   }

   private static void writeMetadata(ComponentMetadataRepo repo, String metadataFile) throws IOException {
      File file = new File(metadataFile);
      File parent = file.getParentFile();
      if (!parent.exists() && !parent.mkdirs()) {
         throw new IllegalStateException("Couldn't create dir: " + parent);
      }

      FileOutputStream fileOutputStream = new FileOutputStream(file);
      BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
      ObjectOutputStream objectOutputStream = new ObjectOutputStream(bufferedOutputStream);
      objectOutputStream.writeObject(repo.componentMetadataMap);
      objectOutputStream.writeObject(repo.factories);
      objectOutputStream.flush();
      objectOutputStream.close();
      bufferedOutputStream.flush();
      bufferedOutputStream.close();
      fileOutputStream.flush();
      fileOutputStream.close();
      System.out.printf(" [ComponentMetadataPersister] Persisted metadata in %s%n", metadataFile);
   }
}
