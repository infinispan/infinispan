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

package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.ReplListener;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

@Test(groups = "functional", testName = "client.hotrod.HotRodAsyncReplicationTest")
public class HotRodAsyncReplicationTest extends MultiHotRodServersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.REPL_ASYNC, false));
      builder.clustering().async().replQueueInterval(1000L).useReplQueue(true);
      builder.eviction().maxEntries(3);

      createHotRodServers(2, builder);
   }

   public void testPutKeyValue(Method m) {
      final RemoteCache<Object, Object> remoteCache0 = client(0).getCache();
      final RemoteCache<Object, Object> remoteCache1 = client(1).getCache();

      ReplListener replList0 = getReplListener(0);
      ReplListener replList1 = getReplListener(1);

      replList0.expect(PutKeyValueCommand.class);
      replList1.expect(PutKeyValueCommand.class);

      final String v1 = v(m);
      remoteCache0.put(1, v1);

      replList0.waitForRpc();
      replList1.waitForRpc();

      assertEquals(v1, remoteCache1.get(1));
      assertEquals(v1, remoteCache1.get(1)); // Called twice to cover all round robin options
      assertEquals(v1, remoteCache0.get(1));
      assertEquals(v1, remoteCache0.get(1)); // Called twice to cover all round robin options
   }

   private ReplListener getReplListener(int cacheIndex) {
      ReplListener replList = listeners.get(cache(cacheIndex));
      if (replList == null)
         replList = new ReplListener(cache(cacheIndex), true, true);
      else
         replList.reconfigureListener(true, true);
         
      return replList;
   }

}
