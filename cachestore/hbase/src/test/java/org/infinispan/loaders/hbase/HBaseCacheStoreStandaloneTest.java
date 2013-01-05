/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.infinispan.loaders.hbase;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.loaders.hbase.test.HBaseCluster;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "manual", testName = "loaders.hbase.HBaseCacheStoreTestStandalone")
public class HBaseCacheStoreStandaloneTest extends SingleCacheManagerTest {

   private static final boolean USE_EMBEDDED = true;

   private static final String[] keys = { "message1", "message2", "message3", "message4",
            "message5" };

   private static final StringObject[] stringObjects = { new StringObject("color", "red", keys[0]),
            new StringObject("color", "blue", keys[1]),
            new StringObject("color", "green", keys[2]),
            new StringObject("color", "black", keys[3]),
            new StringObject("color", "purple", keys[4]), };

   private static Cache<Object, Object> CACHE = null;

   private HBaseCluster hBaseCluster;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      System.setProperty("java.net.preferIPv4Stack", "true");

      if (USE_EMBEDDED)
         return TestCacheManagerFactory.fromXml("hbase-cachestore-ispn-embedded.xml");
      else
         return TestCacheManagerFactory.fromXml("hbase-cachestore-ispn.xml");
   }

   @Override
   protected void setup() throws Exception {
      if (USE_EMBEDDED)
         hBaseCluster = new HBaseCluster();

      super.setup(); // Create the cache manager
   }

   @Override
   protected void teardown() {
      super.teardown(); // Stop cache manager

      if (USE_EMBEDDED)
         HBaseCluster.shutdown(hBaseCluster);
   }

   /**
    * This tests the HBase cachestore, putting and removing items.
    * 
    * @throws Exception
    */
   public void testCache() throws Exception {
      assert CACHE.isEmpty();

      try {
         // add some entries
         for (int i = 0; i < keys.length; i++) {
            CACHE.put(keys[i], stringObjects[i]);
         }

         // test size()
         assert CACHE.size() == keys.length;

         // test keySet()
         Set<Object> storedKeys = CACHE.keySet();
         assert storedKeys.size() == keys.length;
         for (String key : keys) {
            assert storedKeys.contains(key);
         }

         // test entrySet()
         Set<Entry<Object, Object>> storedObjects = CACHE.entrySet();
         assert storedObjects.size() == keys.length;
         List<Object> objects = new ArrayList<Object>();
         for (Entry<Object, Object> entry : storedObjects) {
            objects.add(entry.getValue());
         }
         for (int i = 0; i < keys.length; i++) {
            assert objects.contains(stringObjects[i]);
         }

         // verify that the entries are there and then remove them
         for (int i = 0; i < keys.length; i++) {
            assert CACHE.containsKey(keys[i]);

            Object val = CACHE.get(keys[i]);
            assert val != null;
            assert stringObjects[i].toString().equals(val.toString());
            CACHE.remove(keys[i]);

            val = CACHE.get(keys[i]);
            assert val == null;
         }
      } catch (Exception ex) {
         System.err.println("Caught exception: " + ex.getMessage());
         ex.printStackTrace();
         throw ex;
      }
   }

   public void testRemove() throws Exception {
      for (int i = 0; i < keys.length; i++) {
         CACHE.put(keys[i], stringObjects[i]);
      }

      // verify that the entries are there
      for (int i = 0; i < keys.length; i++) {
         assert CACHE.containsKey(keys[i]);
         Object val = CACHE.get(keys[i]);
         assert stringObjects[i].toString().equals(val.toString());
      }

      boolean result = CACHE.remove(keys[0], stringObjects[0]);
      assert result;

      result = CACHE.remove("zzz", stringObjects[0]);
      assert !result;
   }

   public void testPutAll() throws Exception {
      Map<Object, Object> cacheMap = new HashMap<Object, Object>();
      for (int i = 0; i < keys.length; i++) {
         cacheMap.put(keys[i], stringObjects[i]);
      }

      CACHE.putAll(cacheMap);

      // verify that the entries are there
      for (int i = 0; i < keys.length; i++) {
         assert CACHE.containsKey(keys[i]);
         Object val = CACHE.get(keys[i]);
         assert stringObjects[i].toString().equals(val.toString());
      }
   }

   public void testClear() throws Exception {
      for (int i = 0; i < keys.length; i++) {
         CACHE.put(keys[i], stringObjects[i]);
      }
      for (String key : keys) {
         assert CACHE.containsKey(key);
      }

      CACHE.clear();

      for (String key : keys) {
         assert !CACHE.containsKey(key);
      }
      assert CACHE.keySet().size() == 0;
   }

   public void testReplace() throws Exception {
      CACHE.put(keys[0], stringObjects[0]);
      Object val = CACHE.get(keys[0]);
      assert val != null;
      assert stringObjects[0].toString().equals(val.toString());

      CACHE.replace(keys[0], stringObjects[1]);
      val = CACHE.get(keys[0]);
      assert val != null;
      assert stringObjects[1].toString().equals(val.toString());

      CACHE.remove(keys[0]);
   }

   /**
    * This class represents an object that will be stored in the cache.
    *
    * @author Justin Hayes
    * @since 5.2
    */
   static class StringObject implements Serializable {

      private static final long serialVersionUID = 254191608570966230L;

      protected final String key;
      protected final String value;
      protected final String docId;

      public StringObject(String key, String value, String docId) {
         this.key = key;
         this.value = value;
         this.docId = docId;
      }

      @Override
      public String toString() {
         return key + "[" + value + "]" + (this.docId != null ? "-->" + this.docId : "");
      }
   }

}

