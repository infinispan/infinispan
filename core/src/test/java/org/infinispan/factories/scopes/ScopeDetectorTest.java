/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.factories.scopes;

import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

/**
 * @author Manik Surtani
 * @since 4.0
 */
@Test(groups = "unit", testName = "factories.scopes.ScopeDetectorTest")
public class ScopeDetectorTest extends AbstractInfinispanTest {
   public void testScopeOnClass() {
      testScopes(Test1.class, Scopes.GLOBAL);

   }

   public void testScopeOnInterface() {
      testScopes(Test2.class, Scopes.GLOBAL);
   }

   public void testScopeOnSuperClass() {
      testScopes(Test3.class, Scopes.GLOBAL);
   }

   public void testScopeOnSuperInterface() {
      testScopes(Test4.class, Scopes.GLOBAL);
   }

   public void testNoScopes() {
      testScopes(Test6.class, Scopes.NAMED_CACHE);
   }

   private void testScopes(Class clazz, Scopes expected) {
      Scopes detected = ScopeDetector.detectScope(clazz);
      assert detected == expected : "Expected " + expected + " but was " + detected;
   }

   public static interface Unscoped {

   }

   @Scope(Scopes.GLOBAL)
   public static interface Scoped {

   }

   @Scope(Scopes.GLOBAL)
   public static class SuperScoped {

   }

   public static class SuperUnScoped {

   }

   @Scope(Scopes.GLOBAL)
   public static class Test1 {

   }

   public static class Test2 implements Scoped {

   }

   public static class Test3 extends SuperScoped {

   }

   public static class Test4 extends Test2 {

   }

   public static class Test6 extends SuperUnScoped implements Unscoped {

   }
}
