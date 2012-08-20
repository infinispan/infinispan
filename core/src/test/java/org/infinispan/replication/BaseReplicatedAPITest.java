/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.replication;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.config.Configuration;
import static org.infinispan.context.Flag.CACHE_MODE_LOCAL;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

@Test(groups = "functional", testName = "replication.BaseReplicatedAPITest")
public abstract class BaseReplicatedAPITest extends MultipleCacheManagersTest {

   protected boolean isSync;

   protected void createCacheManagers() throws Throwable {
      Configuration c = getDefaultClusteredConfig(isSync ? Configuration.CacheMode.REPL_SYNC : Configuration.CacheMode.REPL_ASYNC, true);
      c.setStateRetrievalTimeout(5000);
      createClusteredCaches(2, "replication", c);      
   }

   public void put() {
      AdvancedCache cache1 = cache(0,"replication").getAdvancedCache();
      AdvancedCache cache2 = cache(1,"replication").getAdvancedCache();
      // test a simple put!
      assert cache1.get("key") == null;
      assert cache2.get("key") == null;

      replListener(cache2).expect(PutKeyValueCommand.class);
      cache1.put("key", "value");
      replListener(cache2).waitForRpc();

      assert cache1.get("key").equals("value");
      assert cache2.get("key").equals("value");

      Map map = new HashMap();
      map.put("key2", "value2");
      map.put("key3", "value3");

      replListener(cache2).expect(PutMapCommand.class);
      cache1.putAll(map);
      replListener(cache2).waitForRpc();

      assert cache1.get("key").equals("value");
      assert cache2.get("key").equals("value");
      assert cache1.get("key2").equals("value2");
      assert cache2.get("key2").equals("value2");
      assert cache1.get("key3").equals("value3");
      assert cache2.get("key3").equals("value3");
   }

   public void remove() {
      AdvancedCache cache1 = cache(0,"replication").getAdvancedCache();
      AdvancedCache cache2 = cache(1,"replication").getAdvancedCache();
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value");
      assert cache2.get("key").equals("value");
      assert cache1.get("key") == null;

      replListener(cache2).expect(RemoveCommand.class);
      cache1.remove("key");
      replListener(cache2).waitForRpc();

      assert cache1.get("key") == null;
      assert cache2.get("key") == null;

      cache1.withFlags(CACHE_MODE_LOCAL).put("key", "value");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value");
      assert cache1.get("key").equals("value");
      assert cache2.get("key").equals("value");

      replListener(cache2).expect(RemoveCommand.class);
      cache1.remove("key");
      replListener(cache2).waitForRpc();

      assert cache1.get("key") == null;
      assert cache2.get("key") == null;
   }

   public void testPutIfAbsent() {
      AdvancedCache cache1 = cache(0,"replication").getAdvancedCache();
      AdvancedCache cache2 = cache(1,"replication").getAdvancedCache();
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "valueOld");
      assert cache2.get("key").equals("valueOld");
      assert cache1.get("key") == null;

      replListener(cache2).expect(PutKeyValueCommand.class);
      cache1.putIfAbsent("key", "value");
      replListener(cache2).waitForRpc();

      assert cache1.get("key").equals("value");
      assert cache2.get("key").equals("value");

      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2");

      assert cache1.get("key").equals("value");
      assert cache2.get("key").equals("value2");

      cache1.putIfAbsent("key", "value3");

      assert cache1.get("key").equals("value");
      assert cache2.get("key").equals("value2"); // should not invalidate cache2!!
   }

   public void testRemoveIfPresent() {
      AdvancedCache cache1 = cache(0,"replication").getAdvancedCache();
      AdvancedCache cache2 = cache(1,"replication").getAdvancedCache();
      cache1.withFlags(CACHE_MODE_LOCAL).put("key", "value1");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2");
      assert cache1.get("key").equals("value1");
      assert cache2.get("key").equals("value2");

      cache1.remove("key", "value");

      assert cache1.get("key").equals("value1") : "Should not remove";
      assert cache2.get("key").equals("value2") : "Should not remove";

      replListener(cache2).expect(RemoveCommand.class);
      cache1.remove("key", "value1");
      replListener(cache2).waitForRpc();

      assert cache1.get("key") == null;
      assert cache2.get("key") == null;
   }

   public void testClear() {
      AdvancedCache cache1 = cache(0,"replication").getAdvancedCache();
      AdvancedCache cache2 = cache(1,"replication").getAdvancedCache();
      cache1.withFlags(CACHE_MODE_LOCAL).put("key", "value1");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2");
      assert cache1.get("key").equals("value1");
      assert cache2.get("key").equals("value2");

      replListener(cache2).expect(ClearCommand.class);
      cache1.clear();
      replListener(cache2).waitForRpc();

      assert cache1.get("key") == null;
      assert cache2.get("key") == null;
   }

   public void testReplace() {
      AdvancedCache cache1 = cache(0,"replication").getAdvancedCache();
      AdvancedCache cache2 = cache(1,"replication").getAdvancedCache();
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2");
      assert cache1.get("key") == null;
      assert cache2.get("key").equals("value2");

      cache1.replace("key", "value1"); // should do nothing since there is nothing to replace on cache1

      assert cache1.get("key") == null;
      assert cache2.get("key").equals("value2");

      cache1.withFlags(CACHE_MODE_LOCAL).put("key", "valueN");

      replListener(cache2).expect(ReplaceCommand.class);
      cache1.replace("key", "value1");
      replListener(cache2).waitForRpc();

      assert cache1.get("key").equals("value1");
      assert cache2.get("key").equals("value1");
   }

   public void testReplaceWithOldVal() {
      AdvancedCache cache1 = cache(0,"replication").getAdvancedCache();
      AdvancedCache cache2 = cache(1,"replication").getAdvancedCache();
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2");
      assert cache1.get("key") == null;
      assert cache2.get("key").equals("value2");

      cache1.replace("key", "valueOld", "value1"); // should do nothing since there is nothing to replace on cache1

      assert cache1.get("key") == null;
      assert cache2.get("key").equals("value2");

      cache1.withFlags(CACHE_MODE_LOCAL).put("key", "valueN");

      cache1.replace("key", "valueOld", "value1"); // should do nothing since there is nothing to replace on cache1

      assert cache1.get("key").equals("valueN");
      assert cache2.get("key").equals("value2");

      replListener(cache2).expect(ReplaceCommand.class);
      cache1.replace("key", "valueN", "value1");
      replListener(cache2).waitForRpc();

      assert cache1.get("key").equals("value1");
      assert cache2.get("key").equals("value1");
   }

   public void testLocalOnlyClear() {
      AdvancedCache cache1 = cache(0,"replication").getAdvancedCache();
      AdvancedCache cache2 = cache(1,"replication").getAdvancedCache();
      cache1.withFlags(CACHE_MODE_LOCAL).put("key", "value1");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2");
      assert cache1.get("key").equals("value1");
      assert cache2.get("key").equals("value2");

      cache1.withFlags(CACHE_MODE_LOCAL).clear();

      assert cache1.get("key") == null;
      assert cache2.get("key") != null;
      assert cache2.get("key").equals("value2");
   }
}
