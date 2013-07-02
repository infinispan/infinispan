package org.infinispan.cdi.test.distexec;

import java.lang.reflect.Method;

import org.infinispan.test.MultipleCacheManagersTest;
import org.jboss.arquillian.testng.Arquillian;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

public abstract class MultipleCacheManagersArquillianTest extends Arquillian {

   @BeforeClass(alwaysRun = true)
   public void createBeforeClass() throws Throwable {
      Class<?> clazz = null;
      for (clazz = getDelegate().getClass(); clazz != null; clazz = clazz.getSuperclass()) {
         for (Method m : clazz.getDeclaredMethods()) {
            if (m.isAnnotationPresent(BeforeClass.class)) {
               m.setAccessible(true);
               m.invoke(getDelegate(), (Object[]) null);
            }
         }
      }
   }

   @BeforeMethod(alwaysRun = true)
   public void createBeforeMethod() throws Throwable {
      Class<?> clazz = null;
      for (clazz = getDelegate().getClass(); clazz != null; clazz = clazz.getSuperclass()) {
         for (Method m : clazz.getDeclaredMethods()) {
            if (m.isAnnotationPresent(BeforeMethod.class)) {
               m.setAccessible(true);
               m.invoke(getDelegate(), (Object[]) null);
            }
         }
      }
   }

   @AfterClass(alwaysRun = true)
   public void destroy() throws Throwable {
      Class<?> clazz = null;
      for (clazz = getDelegate().getClass(); clazz != null; clazz = clazz.getSuperclass()) {
         for (Method m : clazz.getDeclaredMethods()) {
            if (m.isAnnotationPresent(AfterClass.class)) {
               m.setAccessible(true);
               m.invoke(getDelegate(), (Object[]) null);
            }
         }
      }
   }

   @AfterMethod(alwaysRun = true)
   public void clearContent() throws Throwable {
      Class<?> clazz = null;
      for (clazz = getDelegate().getClass(); clazz != null; clazz = clazz.getSuperclass()) {
         for (Method m : clazz.getDeclaredMethods()) {
            if (m.isAnnotationPresent(AfterMethod.class)) {
               m.setAccessible(true);
               m.invoke(getDelegate(), (Object[]) null);
            }
         }
      }
   }

   abstract MultipleCacheManagersTest getDelegate();

}
