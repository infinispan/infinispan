package org.infinispan.server.router.profiling.configuration;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.server.router.Router;
import org.infinispan.server.router.profiling.PerfTestConfiguration;
import org.infinispan.server.router.routes.Route;
import org.infinispan.server.router.routes.RouteDestination;
import org.infinispan.server.router.routes.RouteSource;
import org.infinispan.server.router.utils.HotRodClientTestingUtil;

public class SingleServerWithSsl implements PerfTestConfiguration {

    private final String KEYSTORE_LOCATION = getClass().getClassLoader().getResource("sni_server_keystore.jks").getPath();
    private final String TRUSTSTORE_LOCATION = getClass().getClassLoader().getResource("sni_client_truststore.jks").getPath();
    private final char[] PASSWORD = "secret".toCharArray();

    @Override
    public List<HotRodServer> initServers() {
        HotRodServerConfigurationBuilder configuration = new HotRodServerConfigurationBuilder().port(0);
        configuration.ssl()
                .enable()
                .keyStoreFileName(KEYSTORE_LOCATION)
                .keyStorePassword(PASSWORD)
                .trustStoreFileName(TRUSTSTORE_LOCATION)
                .trustStorePassword(PASSWORD);
        return Arrays.asList(HotRodTestingUtil.startHotRodServer(new DefaultCacheManager(), configuration));
    }

    @Override
    public RemoteCacheManager initClient(Optional<Router> router, Optional<Set<Route<? extends RouteSource, ? extends RouteDestination>>> routes, List<HotRodServer> servers) {
        int port = servers.get(0).getTransport().getPort();
        return HotRodClientTestingUtil.createWithSsl(InetAddress.getLoopbackAddress(), port, TRUSTSTORE_LOCATION, PASSWORD);
    }

}
