package org.infinispan.server.test.junit5;

import static org.junit.platform.commons.support.AnnotationSupport.findAnnotatedFields;

import java.lang.reflect.Field;
import java.util.List;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.platform.commons.support.ModifierSupport;
import org.junit.platform.commons.support.ReflectionSupport;

public class CustomInfinispanExtension implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback, AfterAllCallback {

   public static final String EXTENSION_OVERRIDE = "org.infinispan.server.test.junit5.extension";

   private static void invokeMethodIfPresent(ExtensionContext context, String methodName) {
      invokeMethodIfPresent(findAnnotatedFields(context.getRequiredTestClass(), InfinispanServer.class,
            ModifierSupport::isStatic), context, methodName);
   }

   private static void invokeMethodIfPresent(List<Field> fields, ExtensionContext context, String methodName) {
      fields.forEach(f -> {
         try {
            Object is = f.get(null);
            if (is != null) {
               ReflectionSupport.findMethod(is.getClass(), methodName, ExtensionContext.class)
                     .ifPresent(m -> ReflectionSupport.invokeMethod(m, is, context));
            }
         } catch (RuntimeException re) {
            throw re;
         } catch (Exception ex) {
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
      Class<?> testClass = extensionContext.getRequiredTestClass();
      List<Field> fields = findAnnotatedFields(testClass, InfinispanServer.class,
            ModifierSupport::isStatic);

      fields.forEach(f -> {
         try {
            Object is = f.get(null);
            Class<?> fieldType = f.getType();
            // Means the suite didn't define an extension to use - so inject the default
            if (is == null) {
               Class<? extends InfinispanSuite> suiteClass;
               String override = System.getProperty(EXTENSION_OVERRIDE);
               if (override != null) {
                  try {
                     Class<?> possibleClass = Class.forName(override);
                     if (!InfinispanSuite.class.isAssignableFrom(possibleClass)) {
                        throw new IllegalArgumentException("System property defined class name: " + override + " does not extend " +
                              InfinispanSuite.class.getName());
                     }
                     suiteClass = (Class<? extends InfinispanSuite>) possibleClass;
                  } catch (ClassNotFoundException e) {
                     throw new IllegalArgumentException("System property defined class name: " + override + " for extension override is not found!", e);
                  }
               } else {
                  InfinispanServer ann = f.getAnnotation(InfinispanServer.class);
                  suiteClass = ann.value();
               }

               List<Field> extensionFields = findAnnotatedFields(suiteClass, RegisterExtension.class,
                     possibleField -> {
                        try {
                           return ModifierSupport.isStatic(possibleField) && fieldType.isAssignableFrom(possibleField.get(null).getClass());
                        } catch (IllegalAccessException e) {
                           return false;
                        }
                     });
               if (extensionFields.size() != 1) {
                  throw new IllegalStateException("Test " + testClass + " was ran without an explicit Suite explicit RegisterExtension and" +
                        " its default suite class " + suiteClass + " doesn't have a single static" +
                        " @RegisterExtension field that is assignable to type: " + fieldType + ", had " + extensionFields);
               }
               is = extensionFields.get(0).get(null);
               f.set(null, is);
            }
            Object finalIs = is;
            ReflectionSupport.findMethod(is.getClass(), "beforeAll", ExtensionContext.class)
                  .ifPresent(m -> ReflectionSupport.invokeMethod(m, finalIs, extensionContext));
         } catch (Exception ex) {
            throw new RuntimeException(ex);
         }
      });
   }

   @Override
   public void beforeEach(ExtensionContext extensionContext) {
      invokeMethodIfPresent(extensionContext, "beforeEach");
   }
}
