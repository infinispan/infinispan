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
package org.infinispan.interceptors;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.config.Configuration;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author Manik Surtani (<a href="mailto:manik AT jboss DOT org">manik AT jboss DOT org</a>)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@Test(groups = "functional", testName = "interceptors.MarshalledValueInterceptorTest")
public class MarshalledValueInterceptorTest extends AbstractInfinispanTest {
   EmbeddedCacheManager cm;

   @BeforeTest
   public void setUp() {
      cm = TestCacheManagerFactory.createLocalCacheManager(false);
   }

   @AfterTest
   public void tearDown() {
      TestingUtil.killCacheManagers(cm);
      cm = null;
   }

   public void testDefaultInterceptorStack() {
      assert TestingUtil.findInterceptor(cm.getCache(), MarshalledValueInterceptor.class) == null;

      Configuration configuration = new Configuration();
      configuration.setUseLazyDeserialization(true);
      cm.defineConfiguration("someCache", configuration);
      Cache c = cm.getCache("someCache");

      assert TestingUtil.findInterceptor(c, MarshalledValueInterceptor.class) != null;
      TestingUtil.killCaches(c);
   }

   public void testDisabledInterceptorStack() {
      Configuration cfg = new Configuration();
      cfg.setUseLazyDeserialization(false);
      cm.defineConfiguration("a", cfg);
      Cache c = cm.getCache("a");
      assert TestingUtil.findInterceptor(c, MarshalledValueInterceptor.class) == null;
   }

   public void testExcludedTypes() {
      // Strings
      assert MarshalledValue.isTypeExcluded(String.class);
      assert MarshalledValue.isTypeExcluded(String[].class);
      assert MarshalledValue.isTypeExcluded(String[][].class);
      assert MarshalledValue.isTypeExcluded(String[][][].class);

      // primitives
      assert MarshalledValue.isTypeExcluded(void.class);
      assert MarshalledValue.isTypeExcluded(boolean.class);
      assert MarshalledValue.isTypeExcluded(char.class);
      assert MarshalledValue.isTypeExcluded(byte.class);
      assert MarshalledValue.isTypeExcluded(short.class);
      assert MarshalledValue.isTypeExcluded(int.class);
      assert MarshalledValue.isTypeExcluded(long.class);
      assert MarshalledValue.isTypeExcluded(float.class);
      assert MarshalledValue.isTypeExcluded(double.class);

      assert MarshalledValue.isTypeExcluded(boolean[].class);
      assert MarshalledValue.isTypeExcluded(char[].class);
      assert MarshalledValue.isTypeExcluded(byte[].class);
      assert MarshalledValue.isTypeExcluded(short[].class);
      assert MarshalledValue.isTypeExcluded(int[].class);
      assert MarshalledValue.isTypeExcluded(long[].class);
      assert MarshalledValue.isTypeExcluded(float[].class);
      assert MarshalledValue.isTypeExcluded(double[].class);

      assert MarshalledValue.isTypeExcluded(boolean[][].class);
      assert MarshalledValue.isTypeExcluded(char[][].class);
      assert MarshalledValue.isTypeExcluded(byte[][].class);
      assert MarshalledValue.isTypeExcluded(short[][].class);
      assert MarshalledValue.isTypeExcluded(int[][].class);
      assert MarshalledValue.isTypeExcluded(long[][].class);
      assert MarshalledValue.isTypeExcluded(float[][].class);
      assert MarshalledValue.isTypeExcluded(double[][].class);

      assert MarshalledValue.isTypeExcluded(Void.class);
      assert MarshalledValue.isTypeExcluded(Boolean.class);
      assert MarshalledValue.isTypeExcluded(Character.class);
      assert MarshalledValue.isTypeExcluded(Byte.class);
      assert MarshalledValue.isTypeExcluded(Short.class);
      assert MarshalledValue.isTypeExcluded(Integer.class);
      assert MarshalledValue.isTypeExcluded(Long.class);
      assert MarshalledValue.isTypeExcluded(Float.class);
      assert MarshalledValue.isTypeExcluded(Double.class);

      assert MarshalledValue.isTypeExcluded(Boolean[].class);
      assert MarshalledValue.isTypeExcluded(Character[].class);
      assert MarshalledValue.isTypeExcluded(Byte[].class);
      assert MarshalledValue.isTypeExcluded(Short[].class);
      assert MarshalledValue.isTypeExcluded(Integer[].class);
      assert MarshalledValue.isTypeExcluded(Long[].class);
      assert MarshalledValue.isTypeExcluded(Float[].class);
      assert MarshalledValue.isTypeExcluded(Double[].class);

      assert MarshalledValue.isTypeExcluded(Boolean[][].class);
      assert MarshalledValue.isTypeExcluded(Character[][].class);
      assert MarshalledValue.isTypeExcluded(Byte[][].class);
      assert MarshalledValue.isTypeExcluded(Short[][].class);
      assert MarshalledValue.isTypeExcluded(Integer[][].class);
      assert MarshalledValue.isTypeExcluded(Long[][].class);
      assert MarshalledValue.isTypeExcluded(Float[][].class);
      assert MarshalledValue.isTypeExcluded(Double[][].class);
   }

   public void testNonExcludedTypes() {
      assert !MarshalledValue.isTypeExcluded(Object.class);
      assert !MarshalledValue.isTypeExcluded(List.class);
      assert !MarshalledValue.isTypeExcluded(Collection.class);
      assert !MarshalledValue.isTypeExcluded(Map.class);
      assert !MarshalledValue.isTypeExcluded(Date.class);
      assert !MarshalledValue.isTypeExcluded(Thread.class);
      assert !MarshalledValue.isTypeExcluded(Collection.class);
      assert !MarshalledValue.isTypeExcluded(new Object() {
         String blah;
      }.getClass());
   }
}
