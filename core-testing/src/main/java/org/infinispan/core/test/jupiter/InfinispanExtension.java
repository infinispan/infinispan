package org.infinispan.core.test.jupiter;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.protostream.SerializationContextInitializer;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

/**
 * JUnit Jupiter extension that manages embedded Infinispan cluster lifecycle.
 * <p>
 * Triggered by {@link InfinispanCluster} annotation on test classes.
 * <ul>
 *   <li>Creates the cluster once per test class ({@code BeforeAll})</li>
 *   <li>Injects {@link InfinispanContext} into {@link InfinispanResource} fields</li>
 *   <li>Creates a fresh context per test method ({@code BeforeEach})</li>
 *   <li>Cleans up per-test caches after each test ({@code AfterEach})</li>
 *   <li>Destroys the cluster after all tests ({@code AfterAll})</li>
 * </ul>
 *
 * @since 16.2
 */
public class InfinispanExtension implements
      BeforeAllCallback, AfterAllCallback,
      BeforeEachCallback, AfterEachCallback,
      ParameterResolver {

   private static final ExtensionContext.Namespace NAMESPACE =
         ExtensionContext.Namespace.create(InfinispanExtension.class);

   private static final String CLUSTER_KEY = "cluster";
   private static final String CONTEXT_KEY = "context";

   @Override
   public void beforeAll(ExtensionContext extensionContext) throws Exception {
      Class<?> testClass = extensionContext.getRequiredTestClass();
      InfinispanCluster annotation = testClass.getAnnotation(InfinispanCluster.class);
      if (annotation == null) {
         throw new IllegalStateException(
               "@InfinispanCluster annotation is required on " + testClass.getName());
      }

      List<SerializationContextInitializer> scis = resolveContextInitializers(annotation.serializationContext());

      ClusterHandle cluster = new ClusterHandle(
            annotation.numNodes(),
            annotation.controlledTime(),
            annotation.transportStack(),
            annotation.config(),
            scis,
            annotation.globalState());

      extensionContext.getStore(NAMESPACE).put(CLUSTER_KEY, cluster);
   }

   @Override
   public void afterAll(ExtensionContext extensionContext) {
      ClusterHandle cluster = extensionContext.getStore(NAMESPACE)
            .remove(CLUSTER_KEY, ClusterHandle.class);
      if (cluster != null) {
         cluster.close();
      }
   }

   @Override
   public void beforeEach(ExtensionContext extensionContext) throws Exception {
      ClusterHandle cluster = getCluster(extensionContext);
      String testId = buildTestId(extensionContext);
      InfinispanContext ctx = new InfinispanContext(cluster, testId);

      extensionContext.getStore(NAMESPACE).put(CONTEXT_KEY, ctx);

      // Inject into instance fields
      Object testInstance = extensionContext.getRequiredTestInstance();
      injectFields(testInstance, ctx);
   }

   @Override
   public void afterEach(ExtensionContext extensionContext) {
      InfinispanContext ctx = extensionContext.getStore(NAMESPACE)
            .remove(CONTEXT_KEY, InfinispanContext.class);
      if (ctx != null) {
         ctx.cleanup();
      }
   }

   @Override
   public boolean supportsParameter(ParameterContext parameterContext,
                                    ExtensionContext extensionContext) throws ParameterResolutionException {
      return parameterContext.isAnnotated(InfinispanResource.class)
            && parameterContext.getParameter().getType() == InfinispanContext.class;
   }

   @Override
   public Object resolveParameter(ParameterContext parameterContext,
                                  ExtensionContext extensionContext) throws ParameterResolutionException {
      return extensionContext.getStore(NAMESPACE).get(CONTEXT_KEY, InfinispanContext.class);
   }

   private ClusterHandle getCluster(ExtensionContext extensionContext) {
      // Walk up to the class-level store
      ExtensionContext classContext = extensionContext;
      while (classContext.getParent().isPresent() && classContext.getTestMethod().isPresent()) {
         classContext = classContext.getParent().get();
      }
      ClusterHandle cluster = classContext.getStore(NAMESPACE).get(CLUSTER_KEY, ClusterHandle.class);
      if (cluster == null) {
         throw new IllegalStateException("No cluster found. Is @InfinispanCluster present?");
      }
      return cluster;
   }

   private static String buildTestId(ExtensionContext extensionContext) {
      String className = extensionContext.getRequiredTestClass().getSimpleName();
      String methodName = extensionContext.getRequiredTestMethod().getName();
      String displayName = extensionContext.getDisplayName();
      // Include display name for parameterized tests
      if (!(methodName + "()").equals(displayName)) {
         return className + "." + methodName + "." + displayName;
      }
      return className + "." + methodName;
   }

   private static void injectFields(Object testInstance, InfinispanContext ctx) throws IllegalAccessException {
      for (Class<?> clazz = testInstance.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
         for (Field field : clazz.getDeclaredFields()) {
            if (field.isAnnotationPresent(InfinispanResource.class)
                  && field.getType() == InfinispanContext.class) {
               field.setAccessible(true);
               field.set(testInstance, ctx);
            }
         }
      }
   }

   static List<SerializationContextInitializer> resolveContextInitializers(
         Class<? extends SerializationContextInitializer>[] classes) throws Exception {
      List<SerializationContextInitializer> result = new ArrayList<>(classes.length);
      for (Class<? extends SerializationContextInitializer> sciClass : classes) {
         result.add(resolveInstance(sciClass));
      }
      return result;
   }

   private static SerializationContextInitializer resolveInstance(
         Class<? extends SerializationContextInitializer> sciClass) throws Exception {
      // Try static INSTANCE field first (conventional pattern)
      try {
         Field instanceField = sciClass.getDeclaredField("INSTANCE");
         if (java.lang.reflect.Modifier.isStatic(instanceField.getModifiers())) {
            instanceField.setAccessible(true);
            Object instance = instanceField.get(null);
            if (instance != null) {
               return sciClass.cast(instance);
            }
         }
      } catch (NoSuchFieldException ignored) {
      }
      // Fall back to no-arg constructor
      return sciClass.getDeclaredConstructor().newInstance();
   }
}
