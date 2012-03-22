/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
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

package org.infinispan.configuration;

import org.infinispan.api.WithClassLoaderTest;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.marshall.jboss.DefaultContextClassResolver;
import org.testng.annotations.Test;

/**
 * A test that verifies that a class resolver can be configured successfully.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = "functional", testName = "configuration.ClassResolverConfigTest")
public class ClassResolverConfigTest extends WithClassLoaderTest {

   @Override
   protected GlobalConfigurationBuilder createSecondGlobalCfgBuilder(ClassLoader cl) {
      GlobalConfigurationBuilder gcBuilder = super.createSecondGlobalCfgBuilder(cl);
      gcBuilder.serialization().classResolver(new DefaultContextClassResolver(cl));
      return gcBuilder;
   }

   @Override
   @Test(expectedExceptions = AssertionError.class,
         expectedExceptionsMessageRegExp = "Expected a ClassNotFoundException")
   public void testReadingWithCorrectClassLoaderAfterReplication() {
      // With the default context class resolver, if configured correctly,
      // the classloader that we set with the invocation context (i.e.
      // coming from global configuration) is ignored (the super class test
      // has one specific classloader that forces not finding a class), and
      // so the class is found.
      super.testReadingWithCorrectClassLoaderAfterReplication();
   }

}