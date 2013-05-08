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
package org.infinispan.client.hotrod;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.NotifyingFuture;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Future;

import static junit.framework.Assert.assertEquals;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional", testName = "client.hotrod.RemoteAsyncAPITest")
public class RemoteAsyncAPITest extends SingleCacheManagerTest {
   private HotRodServer hotrodServer;
   private RemoteCacheManager rcm;
   private RemoteCache<String, String> c;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager(
            hotRodCacheConfiguration());
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      hotrodServer = TestHelper.startHotRodServer(cacheManager);
      Properties props = new Properties();
      props.put("infinispan.client.hotrod.server_list", "127.0.0.1:" + hotrodServer.getPort());
      props.put("infinispan.client.hotrod.force_return_values","true");
      props.put("testOnBorrow", "false");
      rcm = new RemoteCacheManager(props);
      c = rcm.getCache(true);
   }

   @AfterClass
   @Override
   protected void destroyAfterClass() {
      super.destroyAfterClass();
      killRemoteCacheManager(rcm);
      killServers(hotrodServer);
   }

   public void testAsyncPut() throws Exception {
      // put
      Future<String> f = c.putAsync("k", "v");
      assert f != null;
      assert !f.isCancelled();
      assert f.get() == null;
      assert c.get("k").equals("v");


      f = c.putAsync("k", "v2");
      assert f != null;
      assert !f.isCancelled();
      assert f.get().equals("v") : "Obtained " + f.get();
      assert c.get("k").equals("v2");
   }

   public void testAsyncPutAll() throws Exception {
      Future<Void> f2 = c.putAllAsync(Collections.singletonMap("k", "v3"));
      assert f2 != null;
      assert !f2.isCancelled();
      assert f2.get() == null;
      assert f2.isDone();
      assert c.get("k").equals("v3");
   }


   public void testAsyncPutIfAbsent() throws Exception {
      Future<Void> f2 = c.putAllAsync(Collections.singletonMap("k", "v3"));
      assert f2.get() == null;
      assert f2.isDone();
      assert c.get("k").equals("v3");
      Future f = c.putIfAbsentAsync("k", "v4");
      assert f != null;
      assert !f.isCancelled();
      assert "v3".equals(f.get()) : "Obtained " + f.get();
      assert f.isDone();
      assert c.get("k").equals("v3");
   }

   public void testRemoveAsync() throws Exception {
      c.put("k","v3");
      Future<String> f = c.removeAsync("k");
      assert f != null;
      assert !f.isCancelled();
      assert f.get().equals("v3");
      assert f.isDone();
      assert c.get("k") == null;
   }

   public void testPutIfAbsentAsync() throws Exception {
      Future f = c.putIfAbsentAsync("k", "v4");
      assert f != null;
      assert !f.isCancelled();
      assert f.get() == null;
      assert f.isDone();
      assert c.get("k").equals("v4");
   }

   public void testAsyncGet() throws Exception {
      // put
      Future<String> f = c.putAsync("k", "v");
      assert f != null;
      assert !f.isCancelled();
      assert f.get() == null;
      assert c.get("k").equals("v");

      f = c.getAsync("k");
      assert f != null;
      assert !f.isCancelled();
      assert f.get().equals("v") : "Obtained " + f.get();
      assert c.get("k").equals("v");
   }

   public void testVersionedRemove() throws Exception {

      c.put("k","v4");
      VersionedValue value = c.getVersioned("k");

      Future<Boolean> f3 = c.removeWithVersionAsync("k", value.getVersion() + 1);
      assert f3 != null;
      assert !f3.isCancelled();
      assert f3.get().equals(false);
      assert f3.isDone();

      assert c.get("k").equals("v4");

      f3 = c.removeWithVersionAsync("k", value.getVersion());
      assert f3 != null;
      assert !f3.isCancelled();
      assert f3.get().equals(true);
      assert f3.isDone();
      assert c.get("k") == null;
   }



   public void testReplaceAsync() throws Exception {
      Future f = c.replaceAsync("k", "v5");
      assert f != null;
      assert !f.isCancelled();
      assert f.get() == null;
      assert c.get("k") == null;
      assert f.isDone();

      c.put("k", "v");
      f = c.replaceAsync("k", "v5");
      assert f != null;
      assert !f.isCancelled();
      assert f.get().equals("v");
      assert c.get("k").equals("v5");
      assert f.isDone();
   }

   public void testVersionedReplace() throws Exception {
      assert null == c.replace("aKey", "aValue");


      c.put("aKey", "aValue");
      VersionedValue valueBinary = c.getVersioned("aKey");
      NotifyingFuture<Boolean> future = c.replaceWithVersionAsync("aKey", "aNewValue", valueBinary.getVersion());
      assert !future.isCancelled();
      assert future.get();
      assert future.isDone();

      VersionedValue entry2 = c.getVersioned("aKey");
      assert entry2.getVersion() != valueBinary.getVersion();
      assertEquals(entry2.getValue(), "aNewValue");

      assert !c.replaceWithVersion("aKey", "aNewValue", valueBinary.getVersion());

   }
}
