package org.infinispan.server.test.jmx.suppress.statetransfer;

import static org.infinispan.server.test.util.ITestUtils.SERVER1_MGMT_PORT;
import static org.infinispan.server.test.util.ITestUtils.SERVER2_MGMT_PORT;
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
 * @author <a href="mailto:amanukya@redhat.com">Anna Manukyan</a>
 * @author <a href="mailto:vchepeli@redhat.com">Vitalii Chepeliuk</a>
 */
@RunWith(Arquillian.class)
public class StateTransferSuppressForHotrodIT extends AbstractStateTransferSuppressIT {
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

        providers.add(new MBeanServerConnectionProvider(server(0).getHotrodEndpoint().getInetAddress().getHostName(), SERVER1_MGMT_PORT));
        providers.add(new MBeanServerConnectionProvider(server(1).getHotrodEndpoint().getInetAddress().getHostName(), SERVER2_MGMT_PORT));
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
    protected void createNewProvider(int idx) {
        providers.add(new MBeanServerConnectionProvider(server(idx).getHotrodEndpoint().getInetAddress().getHostName(), SERVER1_MGMT_PORT + idx * 100));
    }
}
