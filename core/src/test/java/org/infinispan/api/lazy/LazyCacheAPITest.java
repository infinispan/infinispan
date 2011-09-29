/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.api.lazy;

import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.lang.reflect.Method;

/**
 * Cache API test with lazy deserialization turned on.
 *
 * @author Galder Zamarre�o
 * @since 4.1
 */
@Test(groups = "functional", testName = "api.lazy.LazyCacheAPITest")
public class LazyCacheAPITest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      // start a single cache instance
      Configuration c = getDefaultStandaloneConfig(true);
      c.setUseLazyDeserialization(true);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createLocalCacheManager(false);
      cm.defineConfiguration("lazy-cache-test", c);
      cache = cm.getCache("lazy-cache-test");
      return cm;
   }

   public void testReplace(Method m) {
      CustomPojo key = new CustomPojo(m.getName());
      cache.put(key, "1");
      assert "1".equals(cache.get(new CustomPojo(m.getName())));
      Object oldValue = cache.replace(new CustomPojo(m.getName()), "2");
      assert "1".equals(oldValue);
      assert "2".equals(cache.get(new CustomPojo(m.getName())));
   }

   public void testReplaceWithOld(Method m) {
      CustomPojo key = new CustomPojo(m.getName());
      cache.put(key, "1");
      assert "1".equals(cache.get(new CustomPojo(m.getName())));
      assert !cache.replace(new CustomPojo(m.getName()), "99", "2");
      assert cache.replace(new CustomPojo(m.getName()), "1", "2");

      key = new CustomPojo(m.getName() + "-withCustomValue");
      CustomPojo v1 = new CustomPojo("value1");
      cache.put(key, v1);
      assert v1.equals(cache.get(key));
      CustomPojo v99 = new CustomPojo("value99");
      CustomPojo v2 = new CustomPojo("value2");
      assert !cache.replace(key, v99, v2);
      assert cache.replace(key, v1, v2);
   }

   public static class CustomPojo implements Serializable {
      static final Log log = LogFactory.getLog(CustomPojo.class);

      private String name;

      public CustomPojo(String name) {
         this.name = name;
      }

      @Override
      public boolean equals(Object obj) {
         if (obj == null) {
            log.debug("null -> false");
            return false;
         }
         log.debug(obj.getClass());
         if (getClass() != obj.getClass()) {
            log.debug("class not same -> false");
            return false;
         }
         final CustomPojo other = (CustomPojo) obj;
         return this.name.equals(other.name);
      }

      @Override
      public int hashCode() {
         return name.hashCode();
      }

   }

}
