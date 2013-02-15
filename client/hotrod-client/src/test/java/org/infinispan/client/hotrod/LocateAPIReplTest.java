/*
 * Copyright 2013 Red Hat, Inc. and/or its affiliates.
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

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.hotrod.HotRodServer;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "functional" , testName = "client.hotrod.LocateAPIReplTest")
public class LocateAPIReplTest extends LocateAPIDistTest {

   protected CacheMode getCacheMode() {
      return CacheMode.REPL_SYNC;
   }

   public void testLocate() {
      remoteCache.put("key", "value");
      remoteCache.get("key");

      // Should be a collection of all servers
      Collection<String> ownersOnClient = remoteCache.locate("key");

      Set<String> allEndpoints = new HashSet<String>();
      for (HotRodServer hrs: Arrays.asList(hotRodServer1, hotRodServer2, hotRodServer3, hotRodServer4)) {
         allEndpoints.add(hrs.getAddress().toString());
      }

      assertEquals("Client's locate() method did not return all endpoints!",
            allEndpoints, ownersOnClient);
   }
}
