package org.infinispan.multimap.impl;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.multimap.api.embedded.EmbeddedMultimapCacheManagerFactory;
import org.infinispan.multimap.api.embedded.MultimapCache;
import org.infinispan.multimap.api.embedded.MultimapCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.data.Person;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.multimap.impl.MultimapTestUtils.NAMES_KEY;
import static org.infinispan.multimap.impl.MultimapTestUtils.OIHANA;
import static org.infinispan.multimap.impl.MultimapTestUtils.RAMON;
import static org.infinispan.multimap.impl.MultimapTestUtils.JULIEN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Test(groups = "functional", testName = "multimap.EmbeddedMultimapCacheWithDuplicatesTest")
public class EmbeddedMultimapCacheWithDuplicatesTest extends SingleCacheManagerTest {

    private static final String TEST_CACHE_NAME = EmbeddedMultimapCacheWithDuplicatesTest.class.getSimpleName();
    protected MultimapCache<String, Person> multimapCache;

    @Override
    protected EmbeddedCacheManager createCacheManager() throws Exception {
        // start a single cache instance
        EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(MultimapSCI.INSTANCE);
        cm.defineConfiguration(TEST_CACHE_NAME, new ConfigurationBuilder().build());
        MultimapCacheManager multimapCacheManager = EmbeddedMultimapCacheManagerFactory.from(cm);
        multimapCache = multimapCacheManager.get(TEST_CACHE_NAME, true);
        cm.getClassAllowList().addClasses(SuperPerson.class);
        return cm;
    }

    public void testSupportsDuplicates() {
        assertTrue(multimapCache.supportsDuplicates());
    }

    public void testPutDuplicates() {

        await(multimapCache.put(NAMES_KEY, JULIEN)
                .thenCompose(r1 -> multimapCache.put(NAMES_KEY, RAMON).thenCompose(r2 -> multimapCache.get(NAMES_KEY).thenAccept(v -> {
                    assertTrue(v.contains(JULIEN));
                    assertEquals(1, v.stream().filter(n -> n.equals(JULIEN)).count());
                    assertTrue(v.contains(RAMON));
                    assertEquals(1, v.stream().filter(n -> n.equals(RAMON)).count());
                }).thenCompose(r3 -> multimapCache.size()).thenAccept(v -> {
                    assertEquals(2, v.intValue());
                }).thenCompose(r4 -> multimapCache.put(NAMES_KEY, JULIEN).thenCompose(r5 -> multimapCache.get(NAMES_KEY)).thenAccept(v -> {
                    assertTrue(v.contains(JULIEN));
                    assertEquals(2, v.stream().filter(n -> n.equals(JULIEN)).count());
                    assertTrue(v.contains(RAMON));
                    assertEquals(1, v.stream().filter(n -> n.equals(RAMON)).count());
                }).thenCompose(r3 -> multimapCache.size()).thenAccept(v -> {
                    assertEquals(3, v.intValue());
                }).thenCompose(r5 -> multimapCache.put(NAMES_KEY, JULIEN).thenCompose(r6 -> multimapCache.get(NAMES_KEY)).thenAccept(v -> {
                            assertTrue(v.contains(JULIEN));
                            assertEquals(3, v.stream().filter(n -> n.equals(JULIEN)).count());
                            assertTrue(v.contains(RAMON));
                            assertEquals(1, v.stream().filter(n -> n.equals(RAMON)).count());
                        }).thenCompose(r7 -> multimapCache.size()).thenAccept(v -> assertEquals(4, v.intValue()))
                )))));
    }

    public void testRemoveKeyValue() {
        await(
                multimapCache.put(NAMES_KEY, OIHANA)
                        .thenCompose(r1 -> multimapCache.size())
                        .thenAccept(s -> assertEquals(1, s.intValue()))
        );

        await(
                multimapCache.put(NAMES_KEY, OIHANA)
                        .thenCompose(r1 -> multimapCache.size())
                        .thenAccept(s -> assertEquals(2, s.intValue()))
        );

        await(
                multimapCache.put(NAMES_KEY, OIHANA)
                        .thenCompose(r1 -> multimapCache.size())
                        .thenAccept(s -> assertEquals(3, s.intValue()))
        );

        await(
                multimapCache.remove(NAMES_KEY, OIHANA)
                        .thenCompose(r1 -> multimapCache.size())
                        .thenAccept(s -> assertEquals(0, s.intValue()))
        );
    }
}
