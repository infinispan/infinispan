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
package org.infinispan.eviction;

import org.infinispan.config.Configuration;
import org.infinispan.eviction.MarshalledValuesEvictionTest.MockMarshalledValueInterceptor;
import org.infinispan.interceptors.MarshalledValueInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@Test(groups = "functional", testName = "eviction.MarshalledValuesManualEvictionTest")
public class MarshalledValuesManualEvictionTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration cfg = new Configuration();
      cfg.setUseLockStriping(false); // to minimise chances of deadlock in the unit test
      cfg.setUseLazyDeserialization(true);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(cfg);
      cache = cm.getCache();
      StreamingMarshaller marshaller = TestingUtil.extractComponent(cache, StreamingMarshaller.class);
      MockMarshalledValueInterceptor interceptor = new MockMarshalledValueInterceptor(marshaller);
      assert TestingUtil.replaceInterceptor(cache, interceptor, MarshalledValueInterceptor.class);
      return cm;
   }
   
   public void testManualEvictCustomKeyValue() {
      ManualEvictionPojo p1 = new ManualEvictionPojo();
      p1.i = 64;
      ManualEvictionPojo p2 = new ManualEvictionPojo();
      p2.i = 24;
      ManualEvictionPojo p3 = new ManualEvictionPojo();
      p3.i = 97;
      ManualEvictionPojo p4 = new ManualEvictionPojo();
      p4.i = 35;

      cache.put(p1, p2);
      cache.put(p3, p4);
      cache.evict(p1);
      
      MockMarshalledValueInterceptor interceptor = (MockMarshalledValueInterceptor) TestingUtil.findInterceptor(cache, MarshalledValueInterceptor.class);
      assert interceptor.marshalledValueCreated;
   }
   
   public void testEvictPrimitiveKeyCustomValue() {
      ManualEvictionPojo p1 = new ManualEvictionPojo();
      p1.i = 51;
      ManualEvictionPojo p2 = new ManualEvictionPojo();
      p2.i = 78;

      cache.put("key-isoprene", p1);
      cache.put("key-hexastyle", p2);
      cache.evict("key-isoprene");

      MockMarshalledValueInterceptor interceptor = (MockMarshalledValueInterceptor) TestingUtil.findInterceptor(cache, MarshalledValueInterceptor.class);
      assert !interceptor.marshalledValueCreated;
   }

   static class ManualEvictionPojo implements Externalizable {
      int i;

      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         ManualEvictionPojo pojo = (ManualEvictionPojo) o;
         if (i != pojo.i) return false;
         return true;
      }

      public int hashCode() {
         int result;
         result = i;
         return result;
      }

      @Override
      public void writeExternal(ObjectOutput out) throws IOException {
         out.writeInt(i);
      }

      @Override
      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
         i = in.readInt();
      }
   }

}
