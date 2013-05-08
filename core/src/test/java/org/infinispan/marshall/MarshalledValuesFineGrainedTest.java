/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
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

package org.infinispan.marshall;

import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.Serializable;

/**
 * Tests just enabling marshalled values on keys and not values, and vice versa.
 *
 * @author Manik Surtani
 */
@Test(groups = "functional", testName = "marshall.MarshalledValuesFineGrainedTest")
public class MarshalledValuesFineGrainedTest extends AbstractInfinispanTest {
   EmbeddedCacheManager ecm;
   final CustomClass key = new CustomClass("key");
   final CustomClass value = new CustomClass("value");

   @AfterMethod
   public void cleanup() {
      TestingUtil.killCacheManagers(ecm);
      ecm = null;
   }

   public void testStoreAsBinaryOnBoth() {
      Configuration c = new Configuration().fluent().storeAsBinary().storeKeysAsBinary(true).storeValuesAsBinary(true).build();
      ecm = TestCacheManagerFactory.createCacheManager(c);
      ecm.getCache().put(key, value);

      DataContainer dc = ecm.getCache().getAdvancedCache().getDataContainer();

      InternalCacheEntry entry = dc.iterator().next();
      Object key = entry.getKey();
      Object value = entry.getValue();

      assert key instanceof MarshalledValue;
      assert ((MarshalledValue) key).get().equals(this.key);

      assert value instanceof MarshalledValue;
      assert ((MarshalledValue) value).get().equals(this.value);
   }

   public void testStoreAsBinaryOnKeys() {
      Configuration c = new Configuration().fluent().storeAsBinary().storeValuesAsBinary(false).build();
      ecm = TestCacheManagerFactory.createCacheManager(c);
      ecm.getCache().put(key, value);

      DataContainer dc = ecm.getCache().getAdvancedCache().getDataContainer();

      InternalCacheEntry entry = dc.iterator().next();
      Object key = entry.getKey();
      Object value = entry.getValue();

      assert key instanceof MarshalledValue;
      assert ((MarshalledValue) key).get().equals(this.key);

      assert this.value.equals(value);
   }

   public void testStoreAsBinaryOnValues() {
      Configuration c = new Configuration().fluent().storeAsBinary().storeKeysAsBinary(false).build();
      ecm = TestCacheManagerFactory.createCacheManager(c);
      ecm.getCache().put(key, value);

      DataContainer dc = ecm.getCache().getAdvancedCache().getDataContainer();

      InternalCacheEntry entry = dc.iterator().next();
      Object key = entry.getKey();
      Object value = entry.getValue();

      assert this.key.equals(key);

      assert value instanceof MarshalledValue;
      assert ((MarshalledValue) value).get().equals(this.value);
   }

   public void testStoreAsBinaryOnNeither() {
      Configuration c = new Configuration().fluent().storeAsBinary().storeKeysAsBinary(false).storeValuesAsBinary(false).build();
      ecm = TestCacheManagerFactory.createCacheManager(c);
      ecm.getCache().put(key, value);

      DataContainer dc = ecm.getCache().getAdvancedCache().getDataContainer();

      assert value.equals(dc.get(key).getValue());
   }

}

class CustomClass implements Serializable {
   final String val;

   CustomClass(String val) {
      this.val = val;
   }

   public String getVal() {
      return val;
   }

   @Override
   public String toString() {
      return "CustomClass{" +
            "val='" + val + '\'' +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CustomClass that = (CustomClass) o;

      if (val != null ? !val.equals(that.val) : that.val != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return val != null ? val.hashCode() : 0;
   }
}
