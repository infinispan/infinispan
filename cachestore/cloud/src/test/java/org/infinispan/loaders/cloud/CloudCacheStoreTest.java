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
package org.infinispan.loaders.cloud;

import org.infinispan.CacheImpl;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.io.UnclosableObjectInputStream;
import org.infinispan.io.UnclosableObjectOutputStream;
import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.marshall.StreamingMarshaller;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.testng.Assert.assertEquals;

@Test(groups = "unit", testName = "loaders.cloud.CloudCacheStoreTest")
public class CloudCacheStoreTest extends BaseCacheStoreTest {

   private static final String csBucket = "Bucket1";
   private static final String cs2Bucket = "Bucket2";
   protected CacheStore cs2;

   private CacheStore buildCloudCacheStoreWithStubCloudService(String bucketName) throws CacheLoaderException {
      CloudCacheStore cs = new CloudCacheStore();
      CloudCacheStoreConfig cfg = new CloudCacheStoreConfig();
      cfg.setPurgeSynchronously(true);
      cfg.setBucketPrefix(bucketName);
      cfg.setCloudService("transient");
      cfg.setIdentity("unit-test-stub");
      cfg.setPassword("unit-test-stub");
      cfg.setProxyHost("unit-test-stub");
      cfg.setProxyPort("unit-test-stub");

      // TODO remove compress = false once ISPN-409 is closed.
      cfg.setCompress(false);
      cfg.setPurgeSynchronously(true); // for more accurate unit testing
      cs.init(cfg, new CacheImpl("aName"), getMarshaller());
      return cs;
   }

   @Override
   protected CacheStore createCacheStore() throws Exception {
      CacheStore store = buildCloudCacheStoreWithStubCloudService(csBucket);
      store.start();
      return store;
   }

   protected CacheStore createAnotherCacheStore() throws Exception {
      CacheStore store = buildCloudCacheStoreWithStubCloudService(cs2Bucket);
      store.start();
      return store;
   }

   @BeforeMethod
   @Override
   public void setUp() throws Exception {
      super.setUp();
      cs.clear();
      Set entries = cs.loadAll();
      assert entries.isEmpty();
      cs2 = createAnotherCacheStore();
      cs2.clear();
      entries = cs2.loadAll();
      assert entries.isEmpty();
   }


   @AfterMethod
   @Override
   public void tearDown() throws CacheLoaderException {
      for (CacheStore cacheStore : Arrays.asList(cs, cs2)) {
         if (cacheStore != null) {
            cacheStore.clear();
            cacheStore.stop();
         }
      }
      cs = null; cs2 = null;
   }


   @SuppressWarnings("unchecked")
   @Override
   @Test(enabled = false, description = "Disabled until JClouds gains a proper streaming API")
   public void testStreamingAPI() throws CacheLoaderException, IOException {
      cs.store(TestInternalCacheEntryFactory.create("k1", "v1", -1, -1));
      cs.store(TestInternalCacheEntryFactory.create("k2", "v2", -1, -1));
      cs.store(TestInternalCacheEntryFactory.create("k3", "v3", -1, -1));

      StreamingMarshaller marshaller = getMarshaller();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ObjectOutput oo = marshaller.startObjectOutput(out, false, 12);
      try {
         cs.toStream(new UnclosableObjectOutputStream(oo));
      } finally {
         marshaller.finishObjectOutput(oo);
         out.close();
      }

      ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
      ObjectInput oi = marshaller.startObjectInput(in, false);
      try {
         cs2.fromStream(new UnclosableObjectInputStream(oi));
      } finally {
         marshaller.finishObjectInput(oi);
         in.close();
      }

      Set<InternalCacheEntry> set = cs2.loadAll();
      assertEquals(set.size(), 3);
      Set expected = new HashSet();
      expected.add("k1");
      expected.add("k2");
      expected.add("k3");
      for (InternalCacheEntry se : set) assert expected.remove(se.getKey());
      assert expected.isEmpty();
   }

   public void testNegativeHashCodes() throws CacheLoaderException {
      ObjectWithNegativeHashcode objectWithNegativeHashcode = new ObjectWithNegativeHashcode();
      cs.store(TestInternalCacheEntryFactory.create(objectWithNegativeHashcode, "hello", -1, -1));
      InternalCacheEntry ice = cs.load(objectWithNegativeHashcode);
      assert ice.getKey().equals(objectWithNegativeHashcode);
      assert ice.getValue().equals("hello");
   }

   private static class ObjectWithNegativeHashcode implements Serializable {
      String s = "hello";
      private static final long serialVersionUID = 5010691348616186237L;

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         ObjectWithNegativeHashcode blah = (ObjectWithNegativeHashcode) o;
         return !(s != null ? !s.equals(blah.s) : blah.s != null);
      }

      @Override
      public int hashCode() {
         return -700;
      }
   }

   @Override
   @Test(enabled = false, description = "Disabled until we can build the blobstore stub to retain state somewhere.")
   public void testStopStartDoesNotNukeValues() throws InterruptedException, CacheLoaderException {

   }

   @SuppressWarnings("unchecked")
   @Override
   @Test(enabled = false, description = "Disabled until JClouds gains a proper streaming API")
   public void testStreamingAPIReusingStreams() throws CacheLoaderException, IOException {
      cs.store(TestInternalCacheEntryFactory.create("k1", "v1", -1, -1));
      cs.store(TestInternalCacheEntryFactory.create("k2", "v2", -1, -1));
      cs.store(TestInternalCacheEntryFactory.create("k3", "v3", -1, -1));

      StreamingMarshaller marshaller = getMarshaller();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] dummyStartBytes = {1, 2, 3, 4, 5, 6, 7, 8};
      byte[] dummyEndBytes = {8, 7, 6, 5, 4, 3, 2, 1};
      ObjectOutput oo = marshaller.startObjectOutput(out, false, dummyStartBytes.length);
      try {
         oo.write(dummyStartBytes);
         cs.toStream(new UnclosableObjectOutputStream(oo));
         oo.flush();
         oo.write(dummyEndBytes);
      } finally {
         marshaller.finishObjectOutput(oo);
         out.close();
      }

      // first pop the start bytes
      byte[] dummy = new byte[8];
      ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
      ObjectInput oi = marshaller.startObjectInput(in, false);
      try {
         int bytesRead = oi.read(dummy, 0, 8);
         assert bytesRead == 8;
         for (int i = 1; i < 9; i++) assert dummy[i - 1] == i : "Start byte stream corrupted!";
         cs2.fromStream(new UnclosableObjectInputStream(oi));
         bytesRead = oi.read(dummy, 0, 8);
         assert bytesRead == 8;
         for (int i = 8; i > 0; i--) assert dummy[8 - i] == i : "Start byte stream corrupted!";
      } finally {
         marshaller.finishObjectInput(oi);
         in.close();
      }

      Set<InternalCacheEntry> set = cs2.loadAll();
      assertEquals(set.size(), 3);
      Set expected = new HashSet();
      expected.add("k1");
      expected.add("k2");
      expected.add("k3");
      for (InternalCacheEntry se : set) assert expected.remove(se.getKey());
      assert expected.isEmpty();
   }
}