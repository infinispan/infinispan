/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat, Inc. and/or its affiliates, and
 * individual contributors as indicated by the @author tags. See the
 * copyright.txt file in the distribution for a full listing of
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
package org.infinispan.eviction;

import org.infinispan.config.Configuration;
import org.infinispan.eviction.MarshalledValuesEvictionTest.MockMarshalledValueInterceptor;
import org.infinispan.interceptors.MarshalledValueInterceptor;
import org.infinispan.manager.CacheManager;
import org.infinispan.marshall.MarshalledValueTest;
import org.infinispan.marshall.Marshaller;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "eviction.MarshalledValuesEvictionFunctionalTest")
public class MarshalledValuesManualEvictionTest extends SingleCacheManagerTest {

   @Override
   protected CacheManager createCacheManager() throws Exception {
      Configuration cfg = new Configuration();
      cfg.setUseLockStriping(false); // to minimise chances of deadlock in the unit test
      cfg.setUseLazyDeserialization(true);
      CacheManager cm = TestCacheManagerFactory.createCacheManager(cfg);
      cache = cm.getCache();
      Marshaller marshaller = TestingUtil.extractComponent(cache, Marshaller.class);
      MockMarshalledValueInterceptor interceptor = new MockMarshalledValueInterceptor(marshaller);
      assert TestingUtil.replaceInterceptor(cache, interceptor, MarshalledValueInterceptor.class);
      return cm;
   }
   
   public void testManualEvictCustomKeyValue() {
      MarshalledValueTest.Pojo p1 = new MarshalledValueTest.Pojo();
      p1.i = 64;
      MarshalledValueTest.Pojo p2 = new MarshalledValueTest.Pojo();
      p2.i = 24;
      MarshalledValueTest.Pojo p3 = new MarshalledValueTest.Pojo();
      p3.i = 97;
      MarshalledValueTest.Pojo p4 = new MarshalledValueTest.Pojo();
      p4.i = 35;

      cache.put(p1, p2);
      cache.put(p3, p4);
      cache.evict(p1);
      
      MockMarshalledValueInterceptor interceptor = (MockMarshalledValueInterceptor) TestingUtil.findInterceptor(cache, MarshalledValueInterceptor.class);
      assert interceptor.marshalledValueCreated;
   }
   
   public void testEvictPrimitiveKeyCustomValue() {
      MarshalledValueTest.Pojo p1 = new MarshalledValueTest.Pojo();
      p1.i = 51;
      MarshalledValueTest.Pojo p2 = new MarshalledValueTest.Pojo();
      p2.i = 78;

      cache.put("key-isoprene", p1);
      cache.put("key-hexastyle", p2);
      cache.evict("key-isoprene");

      MockMarshalledValueInterceptor interceptor = (MockMarshalledValueInterceptor) TestingUtil.findInterceptor(cache, MarshalledValueInterceptor.class);
      assert !interceptor.marshalledValueCreated;
   }

}
