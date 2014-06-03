package org.infinispan.server.test.cs.file;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.server.test.category.CacheStore;
import org.infinispan.server.test.util.RemoteCacheManagerFactory;
import org.infinispan.server.test.util.RemoteInfinispanMBeans;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * The goal is to test that the file-store is really preserving data after server kills/shutdowns.
 * The path configuration and expiration is tested in ExampleConfigs (we can't test there the server restarts because all the
 * caches in the example configuration have purge=true).
 *
 * @author Martin Gencur
 * 
 */
@RunWith(Arquillian.class)
@Category(CacheStore.class)
public class FileCacheStoreIT {

    final String CONTAINER = "standalone-filecs";

    @InfinispanResource(CONTAINER)
    RemoteInfinispanServer server;

    @ArquillianResource
    ContainerController controller;

    @Test
    public void testSurviveRestart() throws Exception {
        RemoteCacheManagerFactory rcmFactory = new RemoteCacheManagerFactory();
        RemoteInfinispanMBeans s = RemoteInfinispanMBeans.create(server, CONTAINER, "default", "local");

        controller.start(CONTAINER);
        RemoteCache<Object, Object> rc = rcmFactory.createCache(s);

        rc.clear();
        assertNull(rc.get("k1"));
        rc.put("k1", "v1");
        rc.put("k2", "v2");
        rc.put("k3", "v3");
        assertEquals("v1", rc.get("k1"));
        assertEquals("v2", rc.get("k2"));
        assertEquals("v3", rc.get("k3"));
        controller.kill(CONTAINER);
        controller.start(CONTAINER);
        assertEquals("v2", rc.get("k2"));
        assertEquals("v3", rc.get("k3"));
        assertNull(rc.get("k1")); //maxEntries was 2, this entry should be lost as the oldest entries are removed
        controller.stop(CONTAINER);
        rcmFactory.stopManagers();
    }
}
