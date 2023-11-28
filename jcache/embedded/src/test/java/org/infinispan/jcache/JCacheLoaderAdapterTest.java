package org.infinispan.jcache;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.commons.time.TimeService;
import org.infinispan.jcache.embedded.JCacheLoaderAdapter;
import org.infinispan.jcache.util.InMemoryJCacheLoader;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.marshall.persistence.impl.MarshalledEntryFactoryImpl;
import org.infinispan.persistence.DummyInitializationContext;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.EmbeddedTimeService;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link JCacheLoaderAdapter}.
 *
 * @author Roman Chigvintsev
 */
@Test(groups = "unit", testName = "jcache.JCacheLoaderAdapterTest")
public class JCacheLoaderAdapterTest extends AbstractInfinispanTest {
    private static TestObjectStreamMarshaller marshaller;
    private static InitializationContext ctx;

    private JCacheLoaderAdapter<Integer, String> adapter;

    @BeforeClass
    public static void setUpClass() {
        TimeService timeService = new EmbeddedTimeService();
        marshaller = new TestObjectStreamMarshaller();
        MarshallableEntryFactory marshalledEntryFactory = new MarshalledEntryFactoryImpl(marshaller);
        ctx = new DummyInitializationContext() {
            @Override
            public TimeService getTimeService() {
                return timeService;
            }

            @Override
            public MarshallableEntryFactory getMarshallableEntryFactory() {
                return marshalledEntryFactory;
            }
        };
    }

    @AfterClass
    public static void tearDownClass() {
        marshaller.stop();
    }

    @BeforeMethod
    public void setUpMethod() {
        adapter = new JCacheLoaderAdapter<>();
        adapter.init(ctx);
        adapter.setCacheLoader(new InMemoryJCacheLoader<Integer, String>().store(1, "v1").store(2, "v2"));
    }

    public void testLoad() {
        assertNull(adapter.loadEntry(0));

        MarshallableEntry v1Entry = adapter.loadEntry(1);

        assertNotNull(v1Entry);
        assertEquals(1, v1Entry.getKey());
        assertEquals("v1", v1Entry.getValue());

        MarshallableEntry v2Entry = adapter.loadEntry(2);

        assertNotNull(v2Entry);
        assertEquals(2, v2Entry.getKey());
        assertEquals("v2", v2Entry.getValue());
    }

    public void testContains() {
        assertFalse(adapter.contains(0));
        assertTrue(adapter.contains(1));
        assertTrue(adapter.contains(2));
    }
}
