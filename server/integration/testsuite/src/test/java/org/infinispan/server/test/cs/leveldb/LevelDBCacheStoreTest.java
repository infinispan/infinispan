package org.infinispan.server.test.cs.leveldb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map.Entry;

import org.apache.commons.codec.binary.Hex;
import org.infinispan.Cache;
import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.model.RemoteInfinispanCache;
import org.infinispan.arquillian.model.RemoteInfinispanCacheManager;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.marshall.AbstractDelegatingMarshaller;
import org.infinispan.commons.marshall.AbstractMarshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.server.test.category.CacheStore;
import org.infinispan.server.test.util.TestUtil;
import org.infinispan.test.TestingUtil;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests LevelDB cache store.
 * 
 * @author Michal Linhard (mlinhard@redhat.com)
 * 
 */
@RunWith(Arquillian.class)
@Category(CacheStore.class)
public class LevelDBCacheStoreTest {

    @InfinispanResource("leveldb")
    RemoteInfinispanServer server;
    // in suite-client-local this is testsuite/standalone-leveldb-local.xml

    @ArquillianResource
    ContainerController controller;

    public static final String CONTAINER = "leveldb";

    private static File dataDir = new File(TestUtil.SERVER_DATA_DIR + File.separator + "leveldbtestcache");
    private static File expiredDir = new File(TestUtil.SERVER_DATA_DIR + File.separator + "leveldb-expiredtestcache");
    String cacheStoreName = System.getProperty("cachestore.name");

    private static final TestMarshaller clientMarshaller = new TestMarshaller();
    private static final StreamingMarshaller serverMarshaller = getServerMarshaller();

    private void removeDataFilesIfExists() {
        if (dataDir.exists()) {
            TestingUtil.recursiveFileRemove(dataDir);
        }
        if (expiredDir.exists()) {
            TestingUtil.recursiveFileRemove(expiredDir);
        }
    }

    @Test
    public void testDataSurvivesRestart() throws Exception {
        removeDataFilesIfExists();
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
        System.out.println("Stored via Hot Rod:");
        printHex("key1", "1");
        printHex("key2", "2");
        printHex("key3", "3");
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

    private void printHex(String key, String value) throws IOException, InterruptedException {
        byte[] keyBytes = marshall(key);
        byte[] valueBytes = marshall(value);
        System.out.println("key \"" + Hex.encodeHexString(keyBytes) + "\": value \"" + Hex.encodeHexString(valueBytes) + "\"");
    }

    @Test
    public void testDataRetrievableViaLevelDbApi() throws Exception {
        removeDataFilesIfExists();
        controller.start(CONTAINER);
        RemoteInfinispanCacheManager managerJmx = server.getCacheManager("local");
        RemoteInfinispanCache cacheJmx = managerJmx.getCache("testcache");
        RemoteCache<String, String> cache = createManager().getCache();
        cache.clear();
        assertEquals(0, cacheJmx.getNumberOfEntries());
        cache.put("key1", "1");
        assertEquals("1", cache.get("key1"));
        System.out.println("Stored via Hot Rod:");
        printHex("key1", "1");

        assertTrue(dataDir.exists());
        assertTrue(dataDir.isDirectory());
        assertTrue(expiredDir.exists());
        assertTrue(expiredDir.isDirectory());
        controller.stop(CONTAINER);

        DB db = Iq80DBFactory.factory.open(dataDir, new Options());

        System.out.println("LevelDB file " + dataDir.getAbsolutePath() + " contents:");
        DBIterator dbi = db.iterator();
        while (dbi.hasNext()) {
            Entry<byte[], byte[]> entry = dbi.next();
            System.out.println("key \"" + Hex.encodeHexString(entry.getKey()) + "\": value \""
                + Hex.encodeHexString(entry.getValue()) + "\"");
        }

        assertNotNull(db.get(marshall("key1")));
    }

    private byte[] marshall(Object o) throws IOException, InterruptedException {
        return serverMarshaller.objectToByteBuffer(clientMarshaller.objectToByteBuffer(o));
    }

    private static class TestMarshaller extends AbstractMarshaller {

        @Override
        public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
            byte[] bytes = new byte[length];
            System.arraycopy(buf, offset, bytes, 0, length);
            return new String(bytes);
        }

        @Override
        public boolean isMarshallable(Object o) throws Exception {
            return (o instanceof String);
        }

        @Override
        protected ByteBuffer objectToBuffer(Object o, int estimatedSize) throws IOException, InterruptedException {
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
        ConfigurationBuilder cfgBuild = TestUtil.createConfigBuilder(server.getHotrodEndpoint().getInetAddress().getHostName(),
            server.getHotrodEndpoint().getPort());
        if (clientMarshaller != null) {
            cfgBuild.marshaller(clientMarshaller);
        }
        return new RemoteCacheManager(cfgBuild.build());
    }

    private static AbstractDelegatingMarshaller getServerMarshaller() {
        DefaultCacheManager dcm = new DefaultCacheManager();
        dcm.start();
        dcm.getCache();
        return extractCacheMarshaller(dcm.getCache());
    }

    private static Object extractField(Object target, String fieldName) {
        return extractField(target.getClass(), target, fieldName);
    }

    private static Object extractField(Class<?> type, Object target, String fieldName) {
        while (true) {
            Field field;
            try {
                field = type.getDeclaredField(fieldName);
                field.setAccessible(true);
                return field.get(target);
            } catch (Exception e) {
                if (type.equals(Object.class)) {
                    e.printStackTrace();
                    return null;
                } else {
                    // try with superclass!!
                    type = type.getSuperclass();
                }
            }
        }
    }

    public static AbstractDelegatingMarshaller extractCacheMarshaller(Cache<?, ?> cache) {
        ComponentRegistry cr = (ComponentRegistry) extractField(cache, "componentRegistry");
        StreamingMarshaller marshaller = cr.getComponent(StreamingMarshaller.class, KnownComponentNames.CACHE_MARSHALLER);
        return (AbstractDelegatingMarshaller) marshaller;
    }
}