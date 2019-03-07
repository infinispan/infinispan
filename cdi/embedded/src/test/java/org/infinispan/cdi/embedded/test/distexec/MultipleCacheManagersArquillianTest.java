package org.infinispan.cdi.embedded.test.distexec;

import static org.testng.AssertJUnit.fail;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

import org.infinispan.commons.test.ThreadLeakChecker;
import org.infinispan.test.MultipleCacheManagersTest;
import org.jboss.arquillian.testng.Arquillian;
import org.testng.ITestContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

public abstract class MultipleCacheManagersArquillianTest extends Arquillian {

   private void runAnnotatedDelegateMethods(Class<? extends Annotation> annotation, ITestContext ctx) throws Throwable {
      for (Class<?> clazz = getDelegate().getClass(); clazz != null; clazz = clazz.getSuperclass()) {
         for (Method m : clazz.getDeclaredMethods()) {
            if (m.isAnnotationPresent(annotation)) {
               m.setAccessible(true);
               switch (m.getParameterCount()) {
                  case 0:
                     m.invoke(getDelegate(), (Object[]) null);
                     break;
                  case 1:
                     if (m.getParameterTypes()[0].isAssignableFrom(ITestContext.class)) {
                        m.invoke(getDelegate(), new Object[] {ctx});
                        break;
                     }
                  default:
                     fail("Cannot invoke " + annotation.getSimpleName() + " method because of unknown parameters: " + m.getName());
               }

            }
         }
      }
   }

   @BeforeClass(alwaysRun = true)
   public void createBeforeClass(ITestContext ctx) throws Throwable {
      runAnnotatedDelegateMethods(BeforeClass.class, ctx);
   }

   @BeforeMethod(alwaysRun = true)
   public void createBeforeMethod(ITestContext ctx) throws Throwable {
      ThreadLeakChecker.testStarted(getDelegate().getClass().getName());
      runAnnotatedDelegateMethods(BeforeMethod.class, ctx);
   }

   @AfterClass(alwaysRun = true)
   public void destroy(ITestContext ctx) throws Throwable {
      runAnnotatedDelegateMethods(AfterClass.class, ctx);
      ThreadLeakChecker.testFinished(getDelegate().getClass().getName());
   }

   @AfterMethod(alwaysRun = true)
   public void clearContent(ITestContext ctx) throws Throwable {
      runAnnotatedDelegateMethods(AfterMethod.class, ctx);
   }

   abstract MultipleCacheManagersTest getDelegate();

}
