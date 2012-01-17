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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HBaseCacheStoreTestStandalone {

   private static final boolean USE_EMBEDDED = true;

   private static final String[] keys = { "message1", "message2", "message3", "message4",
            "message5" };

   private static final StringObject[] stringObjects = { new StringObject("color", "red", keys[0]),
            new StringObject("color", "blue", keys[1]),
            new StringObject("color", "green", keys[2]),
            new StringObject("color", "black", keys[3]),
            new StringObject("color", "purple", keys[4]), };

   private static Cache<Object, Object> CACHE = null;

   private static EmbeddedServerHelper embedded = null;
   
   static {
      if (USE_EMBEDDED) {
         embedded = new EmbeddedServerHelper();
         
         try {
            embedded.setup();
         } catch (Exception ex) {
            System.err.println(ex.getClass().getName() + " occured starting " +
            		"up embedded HBase server: " + ex.getMessage());
         }
      }
   }
   
   @Before
   public void setup() throws Exception {
      System.setProperty("java.net.preferIPv4Stack", "true");
      
      if (USE_EMBEDDED) {
         CACHE = new DefaultCacheManager("hbase-cachestore-ispn-embedded.xml").getCache();
      } else {
         CACHE = new DefaultCacheManager("hbase-cachestore-ispn.xml").getCache();
      }
   }

   @After
   public void tearDown() throws Exception {
      CACHE.clear();
      CACHE.stop();
   }

   /**
    * This tests the HBase cachestore, putting and removing items.
    * 
    * @throws Exception
    */
   @Test
   public void testCache() throws Exception {
      assertTrue(CACHE.isEmpty());

      try {
         // add some entries
         for (int i = 0; i < keys.length; i++) {
            CACHE.put(keys[i], stringObjects[i]);
         }

         // test size()
         assertEquals(keys.length, CACHE.size());

         // test keySet()
         Set<Object> storedKeys = CACHE.keySet();
         assertEquals(keys.length, storedKeys.size());
         for (int i = 0; i < keys.length; i++) {
            assertTrue(storedKeys.contains(keys[i]));
         }

         // test entrySet()
         Set<Entry<Object, Object>> storedObjects = CACHE.entrySet();
         assertEquals(keys.length, storedObjects.size());
         List<Object> objects = new ArrayList<Object>();
         for (Entry<Object, Object> entry : storedObjects) {
            objects.add(entry.getValue());
         }
         for (int i = 0; i < keys.length; i++) {
            assertTrue(objects.contains(stringObjects[i]));
         }

         // verify that the entries are there and then remove them
         for (int i = 0; i < keys.length; i++) {
            assertTrue(CACHE.containsKey(keys[i]));

            Object val = CACHE.get(keys[i]);
            assertNotNull(val);
            assertEquals(val.toString(), stringObjects[i].toString());
            CACHE.remove(keys[i]);

            val = CACHE.get(keys[i]);
            assertNull(val);
         }
      } catch (Exception ex) {
         System.err.println("Caught exception: " + ex.getMessage());
         ex.printStackTrace();
         throw ex;
      }
   }

   @Test
   public void testRemove() throws Exception {
      for (int i = 0; i < keys.length; i++) {
         CACHE.put(keys[i], stringObjects[i]);
      }

      // verify that the entries are there
      for (int i = 0; i < keys.length; i++) {
         assertTrue(CACHE.containsKey(keys[i]));
         Object val = CACHE.get(keys[i]);
         assertEquals(val.toString(), stringObjects[i].toString());
      }

      boolean result = CACHE.remove(keys[0], stringObjects[0]);
      assertTrue(result);

      result = CACHE.remove("zzz", stringObjects[0]);
      assertTrue(!result);
   }

   @Test
   public void testPutAll() throws Exception {
      Map<Object, Object> cacheMap = new HashMap<Object, Object>();
      for (int i = 0; i < keys.length; i++) {
         cacheMap.put(keys[i], stringObjects[i]);
      }

      CACHE.putAll(cacheMap);

      // verify that the entries are there
      for (int i = 0; i < keys.length; i++) {
         assertTrue(CACHE.containsKey(keys[i]));
         Object val = CACHE.get(keys[i]);
         assertEquals(val.toString(), stringObjects[i].toString());
      }
   }

   @Test
   public void testClear() throws Exception {
      for (int i = 0; i < keys.length; i++) {
         CACHE.put(keys[i], stringObjects[i]);
      }
      for (int i = 0; i < keys.length; i++) {
         assertTrue(CACHE.containsKey(keys[i]));
      }

      CACHE.clear();

      for (int i = 0; i < keys.length; i++) {
         assertTrue(!CACHE.containsKey(keys[i]));
      }
      assertEquals(0, CACHE.keySet().size());
   }

   @Test
   public void testReplace() throws Exception {
      CACHE.put(keys[0], stringObjects[0]);
      Object val = CACHE.get(keys[0]);
      assertNotNull(val);
      assertEquals(val.toString(), stringObjects[0].toString());

      CACHE.replace(keys[0], stringObjects[1]);
      val = CACHE.get(keys[0]);
      assertNotNull(val);
      assertEquals(val.toString(), stringObjects[1].toString());

      CACHE.remove(keys[0]);
   }

}

/**
 * This class represents an object that will be stored in the cache.
 * 
 * @author Justin Hayes
 * @since 5.1
 */
class StringObject implements Serializable {

   private static final long serialVersionUID = 254191608570966230L;

   protected final String key;
   protected final String value;
   protected final String docId;

   public StringObject(String key, String value, String docId) {
      this.key = key;
      this.value = value;
      this.docId = docId;
   }

   public StringObject(String key, String value) {
      this.key = key;
      this.value = value;
      this.docId = null;
   }

   public String getKey() {
      return key;
   }

   public String getValue() {
      return this.value;
   }

   public String getDocId() {
      return docId;
   }

   @Override
   public String toString() {
      return key + "[" + value + "]" + (this.docId != null ? "-->" + this.docId : "");
   }
}
