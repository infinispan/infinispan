/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.xsite;

import org.infinispan.Cache;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "xsite")
public abstract class BaseCacheOperationsTest extends AbstractTwoSitesTest {

   @Test
   public void testRemove() {
      testRemove("LON");
      testRemove("NYC");
   }

   @Test
   public void testPutAndClear() {
      testPutAndClear("LON");
      testPutAndClear("NYC");
   }

   @Test
   public void testReplace() {
      testReplace("LON");
      testReplace("NYC");
   }

   @Test
   public void testPutAll() {
      testPutAll("LON");
      testPutAll("NYC");
   }

   private void testRemove(String site) {
      String key = key(site);
      String val = val(site);

      cache(site, 0).put(key, val);
      assertEquals(backup(site).get(key), val);

      cache(site, 0).remove(key);
      assertNull(backup(site).get(key));

      cache(site, 0).put(key, val);
      assertEquals(backup(site).get(key), val);

      cache(site, 0).remove(key, val);
      assertNull(backup(site).get(key));
   }

   private void testReplace(String site) {
      String key = key(site);
      String val = val(site);
      cache(site, 0).put(key, val);
      Cache<Object, Object> backup = backup(site);
      assertEquals(backup.get(key), val);

      String val2 = val + 1;

      cache(site, 0).replace(key, val2);
      assertEquals(backup.get(key), val2);

      String val3 = val+2;
      cache(site, 0).replace(key, "v_non", val3);
      assertEquals(backup.get(key), val2);

      cache(site, 0).replace(key, val2, val3);
      assertEquals(backup.get(key), val3);
   }

   private void testPutAndClear(String site) {
      String key = key(site);
      String val = val(site);

      cache(site, 0).put(key, val);
      assertEquals(backup(site).get(key), val);

      cache(site, 0).clear();
      assertNull(backup(site).get(key+1));
      assertNull(backup(site).get(key));
   }

   private void testPutAll( String site) {
      Map all = new HashMap();
      String key = key(site);
      String val = val(site);

      for (int i = 0; i < 10; i++) {
         all.put(key + i, val + i);
      }
      cache(site, 0).putAll(all);
      for (int i = 0; i < 10; i++) {
         assertEquals(backup(site).get(key + i), val + i);
      }
   }
}
