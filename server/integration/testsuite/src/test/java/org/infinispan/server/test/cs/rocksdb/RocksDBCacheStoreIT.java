package org.infinispan.server.test.cs.rocksdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.commons.codec.binary.Hex;
import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.model.RemoteInfinispanCache;
import org.infinispan.arquillian.model.RemoteInfinispanCacheManager;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.AbstractMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.server.test.category.CacheStore;
import org.infinispan.server.test.util.ITestUtils;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

/**
 * Tests RocksDB cache store.
 *
 * @author Michal Linhard (mlinhard@redhat.com)
 */
@RunWith(Arquillian.class)
@Category(CacheStore.class)
public class RocksDBCacheStoreIT {
    private static final Log log = LogFactory.getLog(RocksDBCacheStoreIT.class);

    @InfinispanResource("rocksdb")
    RemoteInfinispanServer server;
    // in suite-client-local this is testsuite/standalone-rocksdb-local.xml

    @ArquillianResource
    ContainerController controller;

    public static final String CONTAINER = "rocksdb";

    private static File dataDir = new File(ITestUtils.SERVER_DATA_DIR + File.separator + "rocksdbtestcache");
    private static File expiredDir = new File(ITestUtils.SERVER_DATA_DIR + File.separator + "rocksdb-expiredtestcache");

    private final TestMarshaller clientMarshaller = new TestMarshaller();

    @Before
    @After
    public void removeDataFilesIfExists() {
        Util.recursiveFileRemove(dataDir);
        Util.recursiveFileRemove(expiredDir);
    }

    @Test
    public void testDataSurvivesRestart() {
        controller.start(CONTAINER);
        RemoteInfinispanCacheManager managerJmx = server.getCacheManager("local");
        RemoteInfinispanCache cacheJmx = managerJmx.getCache("testcache");
        RemoteCache<String, String> cache = createManager().getCache();
        cache.clear();
        assertEquals(0, cacheJmx.getNumberOfEntries());
        cache.put("key1", "1");
        cache.put("key2", "2");
        cache.put("key3", "3");
        assertEquals("1", cache.get("key1"));
        assertEquals("2", cache.get("key2"));
        assertEquals("3", cache.get("key3"));
        log.tracef("Stored via Hot Rod:");
        assertTrue(dataDir.exists());
        assertTrue(dataDir.isDirectory());
        assertTrue(expiredDir.exists());
        assertTrue(expiredDir.isDirectory());
        controller.stop(CONTAINER);
        controller.start(CONTAINER);
        assertEquals("1", cache.get("key1"));
        assertEquals("2", cache.get("key2"));
        assertEquals("3", cache.get("key3"));
        controller.stop(CONTAINER);
    }

    @Test
    public void testDataRetrievableViaRocksDbApi() throws Exception {
        controller.start(CONTAINER);
        RemoteInfinispanCacheManager managerJmx = server.getCacheManager("local");
        RemoteInfinispanCache cacheJmx = managerJmx.getCache("testcache");
        RemoteCache<String, String> cache = createManager().getCache();
        cache.clear();
        assertEquals(0, cacheJmx.getNumberOfEntries());
        cache.put("key1", "1");
        assertEquals("1", cache.get("key1"));
        System.out.println("Stored via Hot Rod:");

        assertTrue(dataDir.exists());
        assertTrue(dataDir.isDirectory());
        assertTrue(expiredDir.exists());
        assertTrue(expiredDir.isDirectory());
        controller.stop(CONTAINER);

        RocksDB db = RocksDB.open(new Options(), dataDir.getAbsolutePath());

        log.tracef("RocksDB file " + dataDir.getAbsolutePath() + " contents:");


        for(RocksIterator i = db.newIterator(); i.isValid(); i.next()) {
            log.tracef("key \"" + Hex.encodeHexString(i.key()) + "\": value \""
                + Hex.encodeHexString(i.value()) + "\"");
          assertNotNull(i.value());
       }
    }

    private static class TestMarshaller extends AbstractMarshaller {

        @Override
        public Object objectFromByteBuffer(byte[] buf, int offset, int length) {
            byte[] bytes = new byte[length];
            System.arraycopy(buf, offset, bytes, 0, length);
            return new String(bytes);
        }

        @Override
        public boolean isMarshallable(Object o) {
            return o instanceof String;
        }

        @Override
        protected ByteBuffer objectToBuffer(Object o, int estimatedSize) {
            if (o instanceof String) {
                String str = (String) o;
                byte[] bytes = str.getBytes();
                return new ByteBufferImpl(bytes, 0, bytes.length);
            } else {
                throw new IllegalArgumentException("type not marshallable");
            }
        }
    }

    private RemoteCacheManager createManager() {
        ConfigurationBuilder cfgBuild = ITestUtils.createConfigBuilder(server.getHotrodEndpoint().getInetAddress().getHostName(),
              server.getHotrodEndpoint().getPort());
        cfgBuild.marshaller(clientMarshaller);
        return new RemoteCacheManager(cfgBuild.build());
    }
}
