package org.infinispan.remoting.jgroups;

import org.easymock.classextension.EasyMock;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.CacheManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsChannelLookup;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.jgroups.protocols.UDP;
import org.jgroups.stack.ProtocolStack;
import org.testng.annotations.Test;

import java.util.Properties;

@Test(testName = "remoting.jgroups.ChannelLookupTest", groups = "functional")
public class ChannelLookupTest extends AbstractInfinispanTest {
    public void channelLookupTest() {
        CacheManager cm = null;
        try {
            GlobalConfiguration gc = GlobalConfiguration.getClusteredDefault();
            Properties p = new Properties();
            p.setProperty("channelLookup", DummyLookup.class.getName());
            gc.setTransportProperties(p);
            cm = TestCacheManagerFactory.createCacheManager(gc);
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
        Channel mockChannel;
        Address a = EasyMock.createNiceMock(Address.class);
        ProtocolStack ps = EasyMock.createNiceMock(ProtocolStack.class);

        public DummyLookup() {
            mockChannel = EasyMock.createNiceMock(Channel.class);
            EasyMock.expect(mockChannel.getAddress()).andReturn(a);
            EasyMock.expect(mockChannel.getProtocolStack()).andReturn(ps);
            EasyMock.expect(ps.getTransport()).andReturn(new UDP());
            EasyMock.replay(mockChannel, a, ps);           
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
