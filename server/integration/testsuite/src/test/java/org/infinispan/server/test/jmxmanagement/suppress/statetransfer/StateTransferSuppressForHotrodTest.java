package org.infinispan.server.test.jmxmanagement.suppress.statetransfer;

import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.server.test.util.RemoteCacheManagerFactory;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests verifying that the state transfer suppress works properly for Hotrod server.
 *
 * @author amanukya@redhat.com
 */
@RunWith(Arquillian.class)
public class StateTransferSuppressForHotrodTest extends AbstractStateTransferSuppressTest {
    private static final String CACHE_MANAGER_NAME = "clustered";
    private static final String CACHE_NAME = "default";

    private RemoteCacheManagerFactory rcmFactory;

    private RemoteCache cache1;
    private RemoteCache cache2;

    @Override
    protected void prepare() {
        rcmFactory = new RemoteCacheManagerFactory();
        cache1 = rcmFactory.createCache(mbean(0));
        cache2 = rcmFactory.createCache(mbean(1));

        providers.add(new MBeanServerConnectionProvider(server(0).getHotrodEndpoint().getInetAddress().getHostName(), managementPort));
        providers.add(new MBeanServerConnectionProvider(server(1).getHotrodEndpoint().getInetAddress().getHostName(), managementPort + 100));
    }

    @Override
    protected void destroy() {
        if (null != rcmFactory) {
            rcmFactory.stopManagers();
        }
        rcmFactory = null;
    }

    @Override
    protected void putDataIntoCache(int count) {
        for (int i = 0; i < count; i++) {
            cache1.put("key" + i, "value" + i);
        }

        assertTrue("The size of both caches should be equal.", cache1.size() == cache2.size());
        assertEquals(count, cache1.size());
        assertEquals(count, cache2.size());
    }

    @Override
    protected String getCacheManagerName() {
        return CACHE_MANAGER_NAME;
    }

    @Override
    protected String getCacheName() {
        return CACHE_NAME;
    }

    @Override
    protected void createNewProvider() {
        providers.add(new MBeanServerConnectionProvider(server(2).getHotrodEndpoint().getInetAddress().getHostName(), managementPort + 200));
    }
}
