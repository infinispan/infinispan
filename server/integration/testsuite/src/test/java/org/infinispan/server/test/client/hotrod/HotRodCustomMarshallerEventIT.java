package org.infinispan.server.test.client.hotrod;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.event.ClientCacheEntryCreatedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryModifiedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryRemovedEvent;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.io.ByteBufferFactoryImpl;
import org.infinispan.commons.marshall.AbstractMarshaller;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.server.test.category.HotRodLocal;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.OverProtocol;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests for the HotRod client receiving events serialized with a custom marshaller.
 *
 * @author Galder Zamarreño
 */
@RunWith(Arquillian.class)
@Category(HotRodLocal.class)
public class HotRodCustomMarshallerEventIT {

    private final String TEST_CACHE_NAME = "testcache";

    static RemoteCacheManager remoteCacheManager;
    RemoteCache<Id, Id> remoteCache;

    @InfinispanResource("container1")
    RemoteInfinispanServer server1;

    @Deployment(testable = false, name = "marshaller")
    @TargetsContainer("container1")
    @OverProtocol("jmx-as7")
    public static Archive<?> deploy1() {
        return createArchive();
    }

    @Before
    public void initialize() {
        if (remoteCacheManager == null) {
            Configuration config = createRemoteCacheManagerConfiguration();
            remoteCacheManager = new RemoteCacheManager(config, true);
        }
        remoteCache = remoteCacheManager.getCache(TEST_CACHE_NAME);
    }

    private Configuration createRemoteCacheManagerConfiguration() {
        ConfigurationBuilder config = new ConfigurationBuilder();
        for (RemoteInfinispanServer server : getServers()) {
            config.addServer().host(server.getHotrodEndpoint().getInetAddress().getHostName())
                    .port(server.getHotrodEndpoint().getPort());
        }

        config.marshaller("org.infinispan.server.test.client.hotrod.HotRodCustomMarshallerEventIT$IdMarshaller");
        return config.build();
    }

    @AfterClass
    public static void release() {
        if (remoteCacheManager != null) {
            remoteCacheManager.stop();
        }
    }

    private static Archive<?> createArchive() {
        return ShrinkWrap.create(JavaArchive.class, "marshaller.jar")
                .addClasses(Id.class, IdMarshaller.class)
                .addAsServiceProvider(Marshaller.class, IdMarshaller.class);
    }

    private List<RemoteInfinispanServer> getServers() {
        List<RemoteInfinispanServer> servers = new ArrayList<>();
        servers.add(server1);
        return Collections.unmodifiableList(servers);
    }

    @Test
    public void testEventReceiveBasic() {
        final IdEventListener eventListener = new IdEventListener();
        remoteCache.addClientListener(eventListener);
        try {
            // Created events
            remoteCache.put(new Id(1), new Id(11));
            ClientCacheEntryCreatedEvent<Id> created = eventListener.pollEvent();
            assertEquals(new Id(1), created.getKey());
            remoteCache.put(new Id(2), new Id(22));
            created = eventListener.pollEvent();
            assertEquals(new Id(2), created.getKey());
            // Modified events
            remoteCache.put(new Id(1), new Id(111));
            ClientCacheEntryModifiedEvent<Id> modified = eventListener.pollEvent();
            assertEquals(new Id(1), modified.getKey());
            // Remove events
            remoteCache.remove(new Id(1));
            ClientCacheEntryRemovedEvent<Id> removed = eventListener.pollEvent();
            assertEquals(new Id(1), removed.getKey());
            remoteCache.remove(new Id(2));
            removed = eventListener.pollEvent();
            assertEquals(new Id(2), removed.getKey());
        } finally {
            remoteCache.removeClientListener(eventListener);
        }
    }

    @ClientListener
    public static class IdEventListener {
        public BlockingQueue<ClientEvent> events = new ArrayBlockingQueue<>(128);

        @ClientCacheEntryCreated
        @ClientCacheEntryModified
        @ClientCacheEntryRemoved
        public void handleCreatedEvent(ClientEvent e) {
            events.add(e);
        }

        public <E extends ClientEvent> E pollEvent() {
            try {
                E event = (E) events.poll(10, TimeUnit.SECONDS);
                assertNotNull(event);
                return event;
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }
    }

    public static class Id {
        final byte id;
        public Id(int id) {
            this.id = (byte) id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Id id1 = (Id) o;

            if (id != id1.id) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return id;
        }
    }

    public static class IdMarshaller extends AbstractMarshaller {
        @Override
        protected ByteBuffer objectToBuffer(Object o, int estimatedSize) {
            Id obj = (Id) o;
            ByteBufferFactory factory = new ByteBufferFactoryImpl();
            return factory.newByteBuffer(new byte[]{obj.id}, 0, 1);
        }

        @Override
        public Object objectFromByteBuffer(byte[] buf, int offset, int length) {
            Id id = new Id(buf[0]);
            return id;
        }

        @Override
        public boolean isMarshallable(Object o) throws Exception {
            return true;
        }
    }
}