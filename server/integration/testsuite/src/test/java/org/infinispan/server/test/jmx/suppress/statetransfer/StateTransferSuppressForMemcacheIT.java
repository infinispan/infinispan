package org.infinispan.server.test.jmx.suppress.statetransfer;

import static org.infinispan.server.test.util.ITestUtils.SERVER1_MGMT_PORT;
import static org.infinispan.server.test.util.ITestUtils.SERVER2_MGMT_PORT;
import static org.junit.Assert.assertEquals;

import org.apache.log4j.Logger;
import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.server.test.client.memcached.MemcachedClient;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.runner.RunWith;

/**
 * Verifies the state transfer suppress functionality for Memcached Server.
 *
 * @author <a href="mailto:amanukya@redhat.com">Anna Manukyan</a>
 * @author <a href="mailto:vchepeli@redhat.com">Vitalii Chepeliuk</a>
 */
@RunWith(Arquillian.class)
public class StateTransferSuppressForMemcacheIT extends AbstractStateTransferSuppressIT {
    private static final String CACHE_MANAGER_NAME = "clustered";
    private static final String CACHE_NAME = "memcachedCache";
    private static final Logger log = Logger.getLogger(StateTransferSuppressForMemcacheIT.class);

    private MemcachedClient mc;

    protected void prepare() {
        try {
            mc = new MemcachedClient("UTF-8", server(0).getMemcachedEndpoint().getInetAddress()
                    .getHostName(), server(0).getMemcachedEndpoint().getPort(), server(0).getMemcachedEndpoint().getPort());

            providers.add(new MBeanServerConnectionProvider(server(0).getMemcachedEndpoint().getInetAddress().getHostName(), SERVER1_MGMT_PORT));
            providers.add(new MBeanServerConnectionProvider(server(1).getMemcachedEndpoint().getInetAddress().getHostName(), SERVER2_MGMT_PORT));
        } catch (Exception ex) {
            log.warn("prepare() method throws exception", ex);
        }
    }

    @Override
    protected void destroy() {
        // noop
    }

    @Override
    protected void putDataIntoCache(int count) {
        try {
            for (int i = 0; i < count; i++) {
                mc.set("key" + i, "value" + i);
            }

            long num1 = server(0).getCacheManager(getCacheManagerName()).getCache(getCacheName()).getNumberOfEntries();
            long num2 = server(1).getCacheManager(getCacheManagerName()).getCache(getCacheName()).getNumberOfEntries();

            assertEquals("The size of both caches should be equal.", num1, num2);
            assertEquals(count, num1);
            assertEquals(count, num2);
        } catch (Exception ex) {
            log.error("putDataIntoCache() throws exception", ex);
        }
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
        providers.add(new MBeanServerConnectionProvider(server(idx).getMemcachedEndpoint().getInetAddress().getHostName(), SERVER1_MGMT_PORT + idx * 100));
    }
}