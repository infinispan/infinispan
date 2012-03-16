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

import org.easymock.EasyMock;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.config.Configuration;
import org.infinispan.remoting.ReplicationQueueImpl;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.easymock.EasyMock.*;

@Test(testName = "replication.ReplQueueRestartTest", groups = "unit")
public class ReplQueueRestartTest extends AbstractInfinispanTest {

   @SuppressWarnings("unchecked")
   public void testReplQueueImpl() {
      ReplicationQueueImpl rqi = new ReplicationQueueImpl();
      ScheduledFuture sf = createMock(ScheduledFuture.class);
      expect(sf.cancel(eq(true))).andReturn(true).once();

      ScheduledExecutorService ses = createMock(ScheduledExecutorService.class);
      expect(ses.scheduleWithFixedDelay(EasyMock.<Runnable>anyObject(), anyLong(), anyLong(), EasyMock.<TimeUnit>anyObject()))
         .andReturn(sf).once();

      RpcManager rpc = createNiceMock(RpcManager.class);
      CommandsFactory commandsFactory = createNiceMock(CommandsFactory.class);
      Configuration c = new Configuration();
      c.setUseReplQueue(true);
      replay(ses, rpc, commandsFactory,sf);

      rqi.injectDependencies(ses, rpc, c, commandsFactory);

      rqi.start();

      rqi.stop();

      verify(ses, rpc, commandsFactory, sf);
   }
}
