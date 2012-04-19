/* 
 * JBoss, Home of Professional Open Source 
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tag. All rights reserved. 
 * See the copyright.txt in the distribution for a 
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use, 
 * modify, copy, or redistribute it subject to the terms and conditions 
 * of the GNU Lesser General Public License, v. 2.1. 
 * This program is distributed in the hope that it will be useful, but WITHOUT A 
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A 
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details. 
 * You should have received a copy of the GNU Lesser General Public License, 
 * v.2.1 along with this distribution; if not, write to the Free Software 
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, 
 * MA  02110-1301, USA.
 */
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
