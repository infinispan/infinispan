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
package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.testng.Assert.assertEquals;

/**
 * Tester for https://jira.jboss.org/browse/ISPN-654.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional" , testName="statetransfer.StateTransferLargeObjectTest")
public class StateTransferLargeObjectTest extends MultipleCacheManagersTest {

   private static final Log log = LogFactory.getLog(StateTransferLargeObjectTest.class);
   
   private Cache c0;
   private Cache c1;
   private Cache c2;
   private Cache c3;
   private Map<Integer, BigObject> cache;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC);
      config.setL1CacheEnabled(false);
      config.setNumOwners(3);
      config.setUseLockStriping(false);
      createCluster(config, 4);
      c0 = cache(0);
      c1 = cache(1);
      c2 = cache(2);
      c3 = cache(3);
      waitForClusterToForm();
      log.info("Rehash is complete!");
      cache = new HashMap<Integer, BigObject>();
   }

   public void testForFailure() {
      int num = 1000;
      for (int i = 0; i < num; i++) {
         BigObject bigObject = createBigObject(i, "prefix");
         cache.put(i, bigObject);
         c0.put(i ,bigObject);
      }

      for (int i = 0; i < num; i++) {
         assert c0.get(i) instanceof BigObject;
         assert c1.get(i) instanceof BigObject;
         assert c2.get(i) instanceof BigObject;
         assert c3.get(i) instanceof BigObject;
      }
      log.info("Before stopping a cache!");

      Thread thread = new Thread() {
         @Override
         public void run() {
            log.info("About to stop " + c3.getAdvancedCache().getRpcManager().getAddress());
            c3.stop();
            c3.getCacheManager().stop();
            log.info("Cache stopped async!");
         }
      };
      thread.start();

      int failureCount = 0;


      for (int i = 0; i < num; i++) {
         log.info("----Running a get on " + i);
         try {
            Object o = c0.get(i);
            assertValue(i, o);
         } catch (TimeoutException e) {
            log.error("Exception received", e);
            failureCount++;
         }
         try {
            assertValue(i, c1.get(i));
         } catch (TimeoutException e) {
            failureCount++;
         }
         try {
            assertValue(i, c2.get(i));
         } catch (TimeoutException e) {
            failureCount++;
         }
         if (i % 100 ==0) System.out.println("i = " + i);
      }
      log.info("failureCount = " + failureCount);
      log.info("Before stopping cache managers!");
      TestingUtil.killCacheManagers(manager(2));
      log.info("2 killed");
      TestingUtil.killCacheManagers(manager(1));
      log.info("1 killed");
      TestingUtil.killCacheManagers(manager(0));
      log.info("0 killed");

   }

   private void assertValue(int i, Object o) {
      String msg = " expected some value for " + i + " but got " + o;
      if (!(o instanceof BigObject)) log. error(msg);
      assertEquals(o, cache.get(i));
   }

   private BigObject createBigObject(int num, String prefix) {
      BigObject obj;
      obj = new BigObject();
      obj.setName("[" + num + "|" + prefix + "|" +  (num*3) + "|" + (num*7) + "]");
      obj.setValue(generateLargeString());
      obj.setValue2(generateLargeString());
      obj.setValue3(generateLargeString());
      obj.setValue4(generateLargeString());
      obj.setValue5(generateLargeString());
      obj.setValue6(generateLargeString());
      obj.setValue7(generateLargeString());
      obj.setValue8(generateLargeString());
      obj.setValue9(generateLargeString());
      obj.setValue10(generateLargeString());
      obj.setValue11(generateLargeString());
      obj.setValue12(generateLargeString());
      obj.setValue13(generateLargeString());
      obj.setValue14(generateLargeString());
      obj.setValue15(generateLargeString());
      obj.setValue16(generateLargeString());
      obj.setValue17(generateLargeString());
      obj.setValue18(generateLargeString());
      obj.setValue19(generateLargeString());
      obj.setValue20(generateLargeString());
      return obj;   }

   private String generateLargeString() {
      Random rnd = new Random();
      byte[] bytes = new byte[100];
      rnd.nextBytes(bytes);
      return new String(bytes);
   }


   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() throws Throwable {
   }
}
