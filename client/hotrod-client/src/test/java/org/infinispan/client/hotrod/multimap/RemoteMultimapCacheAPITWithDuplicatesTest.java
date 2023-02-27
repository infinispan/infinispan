package org.infinispan.client.hotrod.multimap;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collection;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.RemoteMultimapCacheAPITWithDuplicatesTest")
public class RemoteMultimapCacheAPITWithDuplicatesTest extends SingleHotRodServerTest {

    private static final String TEST_CACHE_NAME = RemoteMultimapCacheAPITWithDuplicatesTest.class.getSimpleName();
    private RemoteMultimapCache<String, String> multimapCache;

    @Override
    protected RemoteCacheManager getRemoteCacheManager() {
        ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
        // Do not retry when the server response cannot be parsed, see ISPN-12596
        builder.forceReturnValues(isForceReturnValuesViaConfiguration()).maxRetries(0);
        builder.addServer().host("127.0.0.1").port(hotrodServer.getPort());
        return new InternalRemoteCacheManager(builder.build());
    }

    @Override
    protected void setup() throws Exception {
        cacheManager = createCacheManager();
        hotrodServer = createHotRodServer();
        remoteCacheManager = getRemoteCacheManager();
        remoteCacheManager.start();
        cacheManager.defineConfiguration(TEST_CACHE_NAME, new org.infinispan.configuration.cache.ConfigurationBuilder().build() );
        MultimapCacheManager<String, String> rmc = RemoteMultimapCacheManagerFactory.from(remoteCacheManager);

        this.multimapCache = rmc.get(TEST_CACHE_NAME, true);
    }

    protected boolean isForceReturnValuesViaConfiguration() {
        return true;
    }

    public void testSupportsDuplicates() {
        assertTrue(multimapCache.supportsDuplicates());
    }

    public void testPut() {
        multimapCache.put("k", "a").join();
        multimapCache.put("k", "a").join();
        multimapCache.put("k", "a").join();

        Collection<String> kValues = multimapCache.get("k").join();

        assertEquals(3, kValues.size());
        assertTrue(kValues.contains("a"));
    }

    public void testGetWithMetadata() throws Exception {
        multimapCache.put("k", "a").join();
        multimapCache.put("k", "a").join();
        multimapCache.put("k", "c").join();

        MetadataCollection<String> metadataCollection = multimapCache.getWithMetadata("k").join();

        assertEquals(3, metadataCollection.getCollection().size());
        assertTrue(metadataCollection.getCollection().contains("a"));
        assertEquals(-1, metadataCollection.getLifespan());
        assertEquals(0, metadataCollection.getVersion());
    }

    public void testRemoveKeyValue() {
        multimapCache.put("k", "a").join();
        multimapCache.put("k", "a").join();
        multimapCache.put("k", "c").join();
        Collection<String> kValues = multimapCache.get("k").join();
        assertEquals(3, kValues.size());

        assertTrue(multimapCache.remove("k", "a").join());
        assertEquals(1, multimapCache.get("k").join().size());
    }
}
