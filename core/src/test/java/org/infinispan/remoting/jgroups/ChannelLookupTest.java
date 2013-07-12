package org.infinispan.remoting.jgroups;

import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Properties;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
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

@Test(testName = "remoting.jgroups.ChannelLookupTest", groups = "functional")
public class ChannelLookupTest extends AbstractInfinispanTest {
   static Channel mockChannel = mock(Channel.class);
   ProtocolStack ps = mock(ProtocolStack.class);
   Address a = new UUID(1, 1);
   View v = new View(a, 1, Collections.singletonList(a));

   public void channelLookupTest() {

      when(mockChannel.getAddress()).thenReturn(a);
      when(mockChannel.down(isA(Event.class))).thenReturn(a);
      when(mockChannel.getView()).thenReturn(v);
      when(mockChannel.getProtocolStack()).thenReturn(ps);
      when(ps.getTransport()).thenReturn(new UDP());

      EmbeddedCacheManager cm = null;
      try {
         GlobalConfigurationBuilder gc = GlobalConfigurationBuilder.defaultClusteredBuilder();
         gc.transport().defaultTransport().addProperty("channelLookup", DummyLookup.class.getName());

         cm = TestCacheManagerFactory.createClusteredCacheManager(gc, new ConfigurationBuilder());
         cm.start();
         cm.getCache();

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

      @Override
      public Channel getJGroupsChannel(Properties p) {
         return mockChannel;
      }

      @Override
      public boolean shouldStartAndConnect() {
         return false;
      }

      @Override
      public boolean shouldStopAndDisconnect() {
         return false;
      }
   }
}
