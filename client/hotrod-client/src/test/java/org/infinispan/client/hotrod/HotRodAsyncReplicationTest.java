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
import org.infinispan.config.Configuration;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
@Test(groups = "functional", testName = "client.hotrod.HotRodAsyncReplicationTest")
public class HotRodAsyncReplicationTest extends MultiHotRodServersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration cfg = new Configuration().fluent()
         .clustering()
            .mode(Configuration.CacheMode.REPL_ASYNC)
            .async()
               .replQueueInterval(1000L)
               .useReplQueue(true)
         .eviction()
            .maxEntries(3)
         .build();

      createHotRodServers(2, cfg);
   }

   public void testPutKeyValue(Method m) {
      RemoteCache<Object, Object> remoteCache0 = client(0).getCache();
      RemoteCache<Object, Object> remoteCache1 = client(1).getCache();

      replListener(cache(1)).expect(PutKeyValueCommand.class);
      String v1 = v(m);
      remoteCache0.put(1, v1);
      replListener(cache(1)).waitForRpc();
      assertEquals(v1, remoteCache1.get(1));
   }

}
