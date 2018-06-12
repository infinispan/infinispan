package org.infinispan.server.test.query;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.annotations.ProtoSchemaBuilder;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

/**
 * Tests for indexing persistence across server restarts.
 *
 * @since 9.3
 */
@RunWith(Arquillian.class)
public class IndexFlushingIT {

    private static final String SERVER = "query-index-flushing";

    @InfinispanResource(SERVER)
    private RemoteInfinispanServer server;

    @ArquillianResource
    ContainerController controller;

    private RemoteCacheManager remoteCacheManager;

    @Before
    public void before() throws IOException {
        ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
        clientBuilder.addServer()
                .host(server.getHotrodEndpoint().getInetAddress().getHostName())
                .port(server.getHotrodEndpoint().getPort())
                .marshaller(new ProtoStreamMarshaller());

        remoteCacheManager = new RemoteCacheManager(clientBuilder.build());

        SerializationContext serializationContext = ProtoStreamMarshaller.getSerializationContext(remoteCacheManager);
        ProtoSchemaBuilder protoSchemaBuilder = new ProtoSchemaBuilder();
        String protoSchema = protoSchemaBuilder.fileName("transaction.proto")
                .addClass(Transaction.class)
                .build(serializationContext);

        RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
        metadataCache.put("transaction.proto", protoSchema);
    }

    @After
    public void after() {
        if (remoteCacheManager != null) {
            remoteCacheManager.stop();
        }
    }

    @Test
    @WithRunningServer({@RunningServer(name = SERVER)})
    public void testIndexFlushing() {
        RemoteCache<Integer, Transaction> nrtCustomIndexCache = remoteCacheManager.getCache("nrt_custom_index_caches");
        RemoteCache<Integer, Transaction> nrtAutoConfigCache = remoteCacheManager.getCache("nrt_auto_config");

        writeToCache(nrtCustomIndexCache);
        assertIndexSize(nrtCustomIndexCache);

        writeToCache(nrtAutoConfigCache);
        assertIndexSize(nrtAutoConfigCache);

        restartServer();

        assertIndexSize(nrtCustomIndexCache);
        assertIndexSize(nrtAutoConfigCache);
    }

    private void restartServer() {
        controller.stop(SERVER);
        controller.start(SERVER);
    }

    private void writeToCache(RemoteCache<Integer, Transaction> remoteCache) {
        IntStream.range(0, 100).forEach(i -> remoteCache.put(i, new Transaction(i, "script" + i)));
        assertEquals("Cache should be populated", 100, remoteCache.size());
    }

    private void assertIndexSize(RemoteCache<Integer, Transaction> remoteCache) {
        Query query = Search.getQueryFactory(remoteCache).create("From Transaction");
        assertEquals("Entries should be indexed", 100, query.getResultSize());
    }
}
