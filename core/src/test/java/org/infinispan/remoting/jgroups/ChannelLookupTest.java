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
package org.infinispan.remoting.jgroups;

import org.easymock.classextension.EasyMock;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsChannelLookup;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.Event;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.UDP;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.UUID;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Properties;

@Test(testName = "remoting.jgroups.ChannelLookupTest", groups = "functional")
public class ChannelLookupTest extends AbstractInfinispanTest {
   static Channel mockChannel = EasyMock.createNiceMock(Channel.class);
   ProtocolStack ps = EasyMock.createNiceMock(ProtocolStack.class);
   Address a = new UUID(1, 1);
   View v = new View(a, 1, Collections.singletonList(a));

   public void channelLookupTest() {
      EasyMock.reset(mockChannel);
      EasyMock.expect(mockChannel.getAddress()).andReturn(a).atLeastOnce();
      EasyMock.expect(mockChannel.down(EasyMock.isA(Event.class))).andReturn(a).anyTimes();
      EasyMock.expect(mockChannel.getView()).andReturn(v).atLeastOnce();
      EasyMock.expect(mockChannel.getProtocolStack()).andReturn(ps);
      EasyMock.expect(ps.getTransport()).andReturn(new UDP());
      EasyMock.replay(mockChannel, ps);

      EmbeddedCacheManager cm = null;
      try {
         GlobalConfiguration gc = GlobalConfiguration.getClusteredDefault();
         Properties p = new Properties();
         p.setProperty("channelLookup", DummyLookup.class.getName());
         gc.setTransportProperties(p);
         cm = TestCacheManagerFactory.createCacheManager(gc);
         cm.start();
         cm.getCache();
         EasyMock.verify(mockChannel, ps);

         GlobalComponentRegistry gcr = TestingUtil.extractGlobalComponentRegistry(cm);
         Transport t = gcr.getComponent(Transport.class);
         assert t != null;
         assert t instanceof JGroupsTransport;
         assert !(((JGroupsTransport) t).getChannel() instanceof JChannel);
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public static class DummyLookup implements JGroupsChannelLookup {

      public DummyLookup() {
      }

      public Channel getJGroupsChannel(Properties p) {
         return mockChannel;
      }

      public boolean shouldStartAndConnect() {
         return false;
      }

      public boolean shouldStopAndDisconnect() {
         return false;
      }
   }
}
