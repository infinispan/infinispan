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

import java.io.NotSerializableException;

import org.infinispan.commands.write.EvictCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.MarshalledValueInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.marshall.MarshalledValueTest;
import org.infinispan.marshall.Marshaller;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "eviction.MarshalledValuesEvictionTest")
public class MarshalledValuesEvictionTest extends SingleCacheManagerTest {
   
   private static final int CACHE_SIZE=128;


   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration cfg = new Configuration();
      cfg.setEvictionStrategy(EvictionStrategy.FIFO);
      cfg.setEvictionWakeUpInterval(100);
      cfg.setEvictionMaxEntries(CACHE_SIZE); // CACHE_SIZE max entries
      cfg.setUseLockStriping(false); // to minimise chances of deadlock in the unit test
      cfg.setUseLazyDeserialization(true);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(cfg);
      cache = cm.getCache();
      Marshaller marshaller = TestingUtil.extractComponent(cache, Marshaller.class);
      MockMarshalledValueInterceptor interceptor = new MockMarshalledValueInterceptor(marshaller);
      assert TestingUtil.replaceInterceptor(cache, interceptor, MarshalledValueInterceptor.class);
      return cm;
   }
   
   public void testEvictCustomKeyValue() {
      for (int i = 0; i<CACHE_SIZE*2;i++) {
         MarshalledValueTest.Pojo p1 = new MarshalledValueTest.Pojo();
         p1.i = (int)Util.random(2000);
         MarshalledValueTest.Pojo p2 = new MarshalledValueTest.Pojo();
         p2.i = 24;
         cache.put(p1, p2);         
      }   

      // wait for the cache size to drop to CACHE_SIZE, up to a specified amount of time.
      long giveupTime = System.currentTimeMillis() + (1000 * 10); // 10 sec
      while (cache.getAdvancedCache().getDataContainer().size() > CACHE_SIZE && System.currentTimeMillis() < giveupTime) {
         TestingUtil.sleepThread(100);
      }
      
      assert cache.getAdvancedCache().getDataContainer().size() <= CACHE_SIZE : "Expected 1, was " + cache.size(); 

      //let eviction manager kick in
      Util.sleep(3000);
      MockMarshalledValueInterceptor interceptor = (MockMarshalledValueInterceptor) TestingUtil.findInterceptor(cache, MarshalledValueInterceptor.class);
      assert !interceptor.marshalledValueCreated;
   }

   public void testEvictPrimitiveKeyCustomValue() {
      for (int i = 0; i<CACHE_SIZE*2;i++) {
         MarshalledValueTest.Pojo p1 = new MarshalledValueTest.Pojo();
         p1.i = (int)Util.random(2000);
         MarshalledValueTest.Pojo p2 = new MarshalledValueTest.Pojo();
         p2.i = 24;
         cache.put(p1, p2);         
      }

      // wait for the cache size to drop to CACHE_SIZE, up to a specified amount of time.
      long giveupTime = System.currentTimeMillis() + (1000 * 10); // 10 sec
      while (cache.getAdvancedCache().getDataContainer().size() > CACHE_SIZE && System.currentTimeMillis() < giveupTime) {
         TestingUtil.sleepThread(100);
      }
      
      assert cache.getAdvancedCache().getDataContainer().size() <= CACHE_SIZE : "Expected 1, was " + cache.size(); 
      //let eviction manager kick in
      Util.sleep(3000);      
      MockMarshalledValueInterceptor interceptor = (MockMarshalledValueInterceptor) TestingUtil.findInterceptor(cache, MarshalledValueInterceptor.class);
      assert !interceptor.marshalledValueCreated;
   }
   
   static class MockMarshalledValueInterceptor extends MarshalledValueInterceptor {
      boolean marshalledValueCreated;
      
      MockMarshalledValueInterceptor(Marshaller marshaller) {
         injectMarshaller(marshaller);
      }

      @Override
      protected MarshalledValue createMarshalledValue(Object toWrap, InvocationContext ctx)
               throws NotSerializableException {
         marshalledValueCreated = true;
         return super.createMarshalledValue(toWrap, ctx);
      }

      @Override
      public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
         // Reset value so that changes due to invocation can be asserted
         if (marshalledValueCreated) marshalledValueCreated = false;
         return super.visitEvictCommand(ctx, command);
      }
   }

}
