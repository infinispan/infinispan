/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.FutureListener;
import org.infinispan.util.concurrent.NotifyingFutureImpl;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "replication.InvokeRemotelyInFutureTest")
public class InvokeRemotelyInFutureTest extends MultipleCacheManagersTest {

   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      createClusteredCaches(2, "futureRepl", c);   
   }

   public void testInvokeRemotelyInFutureWithListener() throws Exception {

      Cache cache1 = cache(0,"futureRepl");

      //never mind use of KeyValueCommand, the point is that callback is called and completes without Exceptions thrown
      final AtomicBoolean futureDoneOk = new AtomicBoolean();
      NotifyingFutureImpl f = new NotifyingFutureImpl(new Integer(2));
      f.attachListener(new FutureListener<Object>() {
         
         @Override
         public void futureDone(Future<Object> future) {
            try {      
               future.get();
               futureDoneOk.set(true);
            } catch (Exception e) {       
               e.printStackTrace();
            }             
         }
      });
      CommandsFactory cf = cache1.getAdvancedCache().getComponentRegistry().getComponent(CommandsFactory.class);
      PutKeyValueCommand put = cf.buildPutKeyValueCommand("k", "v",
            new EmbeddedMetadata.Builder().build(), null);
      cache1.getAdvancedCache().getRpcManager().invokeRemotelyInFuture(null,
            put, cache1.getAdvancedCache().getRpcManager().getDefaultRpcOptions(true), f);
      TestingUtil.sleepThread(2000);
      assert futureDoneOk.get();   
   }
}
