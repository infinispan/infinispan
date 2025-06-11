package org.infinispan.server.test.junit5;

import static org.junit.platform.commons.support.AnnotationSupport.findAnnotatedFields;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.ModifierSupport;
import org.junit.platform.commons.support.ReflectionSupport;

public class CustomInfinispanExtension implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback, AfterAllCallback {

   private static void invokeMethodIfPresent(ExtensionContext context, String methodName) {
      findAnnotatedFields(context.getRequiredTestClass(), InfinispanServer.class, ModifierSupport::isStatic)
            .forEach(f -> {
               try {
                  Object is = f.get(null);
                  if (is != null) {
                     ReflectionSupport.findMethod(is.getClass(), methodName, ExtensionContext.class)
                           .ifPresent(m -> ReflectionSupport.invokeMethod(m, is, context));
                  }
               }
               catch (Exception ex) {
                  throw new RuntimeException(ex);
               }
            });
   }

   @Override
   public void afterAll(ExtensionContext extensionContext) {
      invokeMethodIfPresent(extensionContext, "afterAll");
   }

   @Override
   public void afterEach(ExtensionContext extensionContext) {
      invokeMethodIfPresent(extensionContext, "afterEach");
   }

   @Override
   public void beforeAll(ExtensionContext extensionContext) {
      invokeMethodIfPresent(extensionContext, "beforeAll");
   }

   @Override
   public void beforeEach(ExtensionContext extensionContext) {
      invokeMethodIfPresent(extensionContext, "beforeEach");
   }
}
