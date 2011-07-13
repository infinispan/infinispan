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
package org.infinispan.replication;

import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.ReplicationQueue;
import org.infinispan.remoting.ReplicationQueueImpl;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "replication.CustomReplQueueTest")
public class CustomReplQueueTest extends SingleCacheManagerTest {
   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration cfg = new Configuration();
      cfg.setCacheMode(Configuration.CacheMode.REPL_ASYNC);
      cfg.setUseReplQueue(true);
      cfg.setReplQueueClass(TestReplQueueClass.class.getName());
      EmbeddedCacheManager ecm = TestCacheManagerFactory.createCacheManager(GlobalConfiguration.getClusteredDefault(), cfg);
      return ecm;
   }

   public void testReplQueueImplType() {
      ReplicationQueue rq = TestingUtil.extractComponent(cache, ReplicationQueue.class);
      assert rq instanceof TestReplQueueClass;              
   }

   public static class TestReplQueueClass extends ReplicationQueueImpl {
   }
}
