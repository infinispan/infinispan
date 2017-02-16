package org.infinispan.server.test.util;

import java.util.ArrayList;
import java.util.Collection;

import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;

/**
 * Keeps collection of {@link RemoteCacheManager} objects, to be able to stop all of them when needed.
 *
 * @author Michal Linhard (mlinhard@redhat.com)
 */
public class RemoteCacheManagerFactory {

    private Collection<RemoteCacheManager> managers;

    public RemoteCacheManagerFactory() {
        this.managers = new ArrayList<RemoteCacheManager>();
    }

    public RemoteCache<Object, Object> createCache(ConfigurationBuilder configBuilder, String cacheName) {
        return createManager(configBuilder).getCache(cacheName);
    }

    public RemoteCache<Object, Object> createCache(RemoteInfinispanMBeans beans) {
        return createCache(beans, ProtocolVersion.DEFAULT_PROTOCOL_VERSION.toString());
    }

    public RemoteCache<Object, Object> createCache(RemoteInfinispanMBeans beans, String protocolVersion) {
        return createManager(beans.server, protocolVersion).getCache(beans.cacheName);
    }

    public RemoteCacheManager createManager(ConfigurationBuilder configBuilder) {
        return addToCollection(new RemoteCacheManager(configBuilder.build()));
    }

    public RemoteCacheManager createManager(RemoteInfinispanMBeans beans) {
        return createManager(beans.server);
    }

    public RemoteCacheManager createManager(RemoteInfinispanServer server) {
        return createManager(server, ProtocolVersion.DEFAULT_PROTOCOL_VERSION.toString());
    }

    private RemoteCacheManager createManager(RemoteInfinispanServer server, String protocolVersion) {
        return addToCollection(ITestUtils.createCacheManager(server, protocolVersion));
    }

    private RemoteCacheManager addToCollection(RemoteCacheManager rcm) {
        managers.add(rcm);
        return rcm;
    }

    /**
     * Stop all managers created by this factory.
     */
    public void stopManagers() {
        for (RemoteCacheManager rcm : managers) {
            rcm.stop();
        }
    }
}
