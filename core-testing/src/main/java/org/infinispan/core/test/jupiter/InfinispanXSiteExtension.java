package org.infinispan.core.test.jupiter;

import java.lang.reflect.Field;
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
 * JUnit Jupiter extension for cross-site Infinispan tests.
 * <p>
 * Triggered by {@link InfinispanXSite} annotation on test classes.
 * <ul>
 *   <li>Creates the multi-site topology once per test class</li>
 *   <li>Injects {@link XSiteContext} into {@link InfinispanResource} fields</li>
 *   <li>Creates a fresh context per test method</li>
 *   <li>Cleans up per-test caches after each test</li>
 *   <li>Destroys all sites after all tests</li>
 * </ul>
 *
 * @since 16.2
 */
public class InfinispanXSiteExtension implements
      BeforeAllCallback, AfterAllCallback,
      BeforeEachCallback, AfterEachCallback,
      ParameterResolver {

   private static final ExtensionContext.Namespace NAMESPACE =
         ExtensionContext.Namespace.create(InfinispanXSiteExtension.class);

   private static final String CLUSTER_KEY = "xsite-cluster";
   private static final String CONTEXT_KEY = "xsite-context";

   @Override
   public void beforeAll(ExtensionContext extensionContext) throws Exception {
      Class<?> testClass = extensionContext.getRequiredTestClass();
      InfinispanXSite annotation = testClass.getAnnotation(InfinispanXSite.class);
      if (annotation == null) {
         throw new IllegalStateException(
               "@InfinispanXSite annotation is required on " + testClass.getName());
      }
      if (annotation.value().length < 2) {
         throw new IllegalStateException(
               "@InfinispanXSite requires at least 2 sites on " + testClass.getName());
      }

      List<SerializationContextInitializer> scis =
            InfinispanExtension.resolveContextInitializers(annotation.serializationContext());

      XSiteClusterHandle cluster = new XSiteClusterHandle(
            annotation.value(), annotation.controlledTime(), scis, annotation.globalState());

      extensionContext.getStore(NAMESPACE).put(CLUSTER_KEY, cluster);
   }

   @Override
   public void afterAll(ExtensionContext extensionContext) {
      XSiteClusterHandle cluster = extensionContext.getStore(NAMESPACE)
            .remove(CLUSTER_KEY, XSiteClusterHandle.class);
      if (cluster != null) {
         cluster.close();
      }
   }

   @Override
   public void beforeEach(ExtensionContext extensionContext) throws Exception {
      XSiteClusterHandle cluster = getCluster(extensionContext);
      String testId = buildTestId(extensionContext);
      XSiteContext ctx = new XSiteContext(cluster, testId);

      extensionContext.getStore(NAMESPACE).put(CONTEXT_KEY, ctx);

      Object testInstance = extensionContext.getRequiredTestInstance();
      injectFields(testInstance, ctx);
   }

   @Override
   public void afterEach(ExtensionContext extensionContext) {
      XSiteContext ctx = extensionContext.getStore(NAMESPACE)
            .remove(CONTEXT_KEY, XSiteContext.class);
      if (ctx != null) {
         ctx.cleanup();
      }
   }

   @Override
   public boolean supportsParameter(ParameterContext parameterContext,
                                    ExtensionContext extensionContext) throws ParameterResolutionException {
      return parameterContext.isAnnotated(InfinispanResource.class)
            && parameterContext.getParameter().getType() == XSiteContext.class;
   }

   @Override
   public Object resolveParameter(ParameterContext parameterContext,
                                  ExtensionContext extensionContext) throws ParameterResolutionException {
      return extensionContext.getStore(NAMESPACE).get(CONTEXT_KEY, XSiteContext.class);
   }

   private XSiteClusterHandle getCluster(ExtensionContext extensionContext) {
      ExtensionContext classContext = extensionContext;
      while (classContext.getParent().isPresent() && classContext.getTestMethod().isPresent()) {
         classContext = classContext.getParent().get();
      }
      XSiteClusterHandle cluster = classContext.getStore(NAMESPACE)
            .get(CLUSTER_KEY, XSiteClusterHandle.class);
      if (cluster == null) {
         throw new IllegalStateException("No xsite cluster found. Is @InfinispanXSite present?");
      }
      return cluster;
   }

   private static String buildTestId(ExtensionContext extensionContext) {
      String className = extensionContext.getRequiredTestClass().getSimpleName();
      String methodName = extensionContext.getRequiredTestMethod().getName();
      String displayName = extensionContext.getDisplayName();
      if (!(methodName + "()").equals(displayName)) {
         return className + "." + methodName + "." + displayName;
      }
      return className + "." + methodName;
   }

   private static void injectFields(Object testInstance, XSiteContext ctx) throws IllegalAccessException {
      for (Class<?> clazz = testInstance.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
         for (Field field : clazz.getDeclaredFields()) {
            if (field.isAnnotationPresent(InfinispanResource.class)
                  && field.getType() == XSiteContext.class) {
               field.setAccessible(true);
               field.set(testInstance, ctx);
            }
         }
      }
   }
}
