package org.infinispan.server.test.client.hotrod;

import java.lang.reflect.Field;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.ServerConfiguration;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.impl.transport.tcp.RequestBalancingStrategy;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransport;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for HotRod client and its RemoteCacheManager API. Subclasses must provide
 * a way to get the list of remote HotRod servers.
 * <p/>
 * Subclasses may be used in Client-Server mode or Hybrid mode where HotRod server
 * runs as a library deployed in an application server.
 *
 * @author Richard Achmatowicz
 * @author Martin Gencur
 * @author Jozef Vilkolak
 */
public abstract class AbstractRemoteCacheManagerIT {

    private static final String IPV6_REGEX = "\\A\\[(.*)\\]:([0-9]+)\\z";
    private static final String IPV4_REGEX = "\\A([^:]+):([0-9]+)\\z";

    private static final String TEST_CACHE_NAME = "testcache";
    private static final String DEFAULT_CLUSTERING_MODE = "dist";
    protected static final String DEFAULT_NAMED_CACHE = "namedCache";

    private static final Log log = LogFactory.getLog(AbstractRemoteCacheManagerIT.class);

    protected abstract List<RemoteInfinispanServer> getServers();

    // creates a configuration with the same values as the hotrod-client.properties files, in ISPN 6.X.Y hotrod-client.properties file will be dropped
    private ConfigurationBuilder createRemoteCacheManagerConfigurationBuilder() {
        ConfigurationBuilder config = new ConfigurationBuilder();
        addServers(config);
        config.balancingStrategy("org.infinispan.server.test.client.hotrod.HotRodTestRequestBalancingStrategy")
                .forceReturnValues(true)
                .tcpNoDelay(false)
                .tcpKeepAlive(true)
                .pingOnStartup(false)
                .transportFactory("org.infinispan.server.test.client.hotrod.HotRodTestTransportFactory")
                .marshaller("org.infinispan.server.test.client.hotrod.HotRodTestMarshaller")
                .asyncExecutorFactory().factoryClass("org.infinispan.server.test.client.hotrod.HotRodTestExecutorFactory")
                .addExecutorProperty("infinispan.client.hotrod.default_executor_factory.pool_size", "20")
                .addExecutorProperty("infinispan.client.hotrod.default_executor_factory.queue_size", "200000")
                .keySizeEstimate(128)
                .valueSizeEstimate(1024);

        if (isDistributedMode()) {
            config.consistentHashImpl(1, "org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashV1");
        }

        return config;
    }

    /*
     * Tests the constructor RemoteCacheManager() - the properties file hotrod-client.properties from classpath is used to
     * define properties - confirm that the file hotrod-client.properties is picked up
     */
    @Test
    public void testDefaultConstructor() throws Exception {
        Configuration conf = createRemoteCacheManagerConfigurationBuilder().build();
        // use the properties file hotrod-client.properties on classpath
        // this properties file contains the test properties with server_list set to ${node0.address}:11222;${node1.address}:11222
        RemoteCacheManager rcm = new RemoteCacheManager();
        RemoteCacheManager rcm2 = new RemoteCacheManager(false);

        assertTrue(rcm.isStarted());
        assertFalse(rcm2.isStarted());

        RemoteCache rc = rcm.getCache(TEST_CACHE_NAME);
        assertEqualConfiguration(conf, rc);
    }

    @Test
    public void testConfigurationConstructors() throws Exception {
        Configuration conf = createRemoteCacheManagerConfigurationBuilder().build();
        RemoteCacheManager rcm = new RemoteCacheManager(conf);
        RemoteCacheManager rcm2 = new RemoteCacheManager(conf, false);
        assertTrue(rcm.isStarted());
        assertFalse(rcm2.isStarted());
        RemoteCache rc = rcm.getCache(TEST_CACHE_NAME);
        assertEqualConfiguration(conf, rc);
    }

    @Test
    public void testEmptyConfiguration() throws Exception {
        ConfigurationBuilder confBuilder = new ConfigurationBuilder();
        addServers(confBuilder);
        RemoteCacheManager rcm = new RemoteCacheManager(confBuilder.build());
        RemoteCache rc = rcm.getCache(TEST_CACHE_NAME);

        ConfigurationBuilder builder = new ConfigurationBuilder();
        addServers(builder);
        builder.balancingStrategy("org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy")
                .forceReturnValues(false)
                .tcpNoDelay(true)
                .pingOnStartup(true)
                .transportFactory("org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory")
                .marshaller("org.infinispan.commons.marshall.jboss.GenericJBossMarshaller")
                .asyncExecutorFactory().factoryClass("org.infinispan.client.hotrod.impl.async.DefaultAsyncExecutorFactory")
                .addExecutorProperty("infinispan.client.hotrod.default_executor_factory.pool_size", "10")
                .addExecutorProperty("infinispan.client.hotrod.default_executor_factory.queue_size", "100000")
                .keySizeEstimate(64)
                .valueSizeEstimate(512);

        if (isDistributedMode()) {
            builder.consistentHashImpl(1, "org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashV1");
        }

        Configuration defaultConf = builder.build();
        assertEqualConfiguration(defaultConf, rc);
        assertTrue(putGetFromDefaultCache(rcm));
    }

    private void addServers(ConfigurationBuilder builder) {
        for (RemoteInfinispanServer server : getServers()) {
            builder.addServer().host(server.getHotrodEndpoint().getInetAddress().getHostName())
                    .port(server.getHotrodEndpoint().getPort());
        }
    }

    protected static boolean isDistributedMode() {
        return getClusteringMode().contains("dist");
    }

    @Test
    public void testStartStop() {
        Configuration cfg = createRemoteCacheManagerConfigurationBuilder().build();

        RemoteCacheManager rcm = new RemoteCacheManager(cfg, false);
        // check initial status
        assertTrue("RemoteCacheManager should not be started initially", !rcm.isStarted());
        // check start status
        rcm.start();
        assertTrue("RemoteCacheManager fails while trying to use it to put/retrieve value", putGetFromDefaultCache(rcm));
        assertTrue("RemoteCacheManager should be started after calling start()", rcm.isStarted());
        // check stopped status
        rcm.stop();
        assertTrue("RemoteCacheManager should be stopped after calling stop()", !rcm.isStarted());
    }

    @Test
    public void testGetCache() {
        RemoteCacheManager rcm = new RemoteCacheManager(createRemoteCacheManagerConfigurationBuilder().build());
        RemoteCache rc1 = rcm.getCache(TEST_CACHE_NAME);

        RemoteCache rc2 = rcm.getCache(DEFAULT_NAMED_CACHE);

        assertNotNull(rc1);
        assertNotNull(rc2);
    }

    @Test
    public void testGetNonExistentCache() {

        // When get named cache which doesn't exists it is created new with default settings
        // but it is not able to get some stats because it is not configured properly
        RemoteCacheManager rcm = new RemoteCacheManager(createRemoteCacheManagerConfigurationBuilder().build());
        RemoteCache rc1 = rcm.getCache("nonExistentCache");

        try {
            for (String stat : rc1.stats().getStatsMap().keySet()) {
                log.tracef(stat + " " + rc1.stats().getStatsMap().get(stat));
            }
            fail("Should throw CacheNotFoundException");
        } catch (Exception e) {
            //ok
        }
    }

    /*
     * Tests the load balancing feature
     *
     * Checks that the default load balancing strategy, RoundRobin, will cycle through the server list as operations are
     * executed.
     *
     * For each operation executed, we would need to obtain its Transport and call getServerAddress() to discover which address
     * was used, but this is difficult to arrange. So instead, we simulate by making repeated calls to
     * TransportFactory.getTransport()
     */
    @Test
    public void testDefaultLoadBalancing() throws Exception {
        if (!isLocalMode()) {
            doTestDefaultLoadBalanding();
        }
    }

    private void doTestDefaultLoadBalanding() throws Exception {
        InetSocketAddress hostport0 = new InetSocketAddress(getServers().get(0).getHotrodEndpoint().getInetAddress().getHostName(), getServers().get(0)
                .getHotrodEndpoint().getPort());
        InetSocketAddress hostport1 = new InetSocketAddress(getServers().get(1).getHotrodEndpoint().getInetAddress().getHostName(), getServers().get(1)
                .getHotrodEndpoint().getPort());

        TcpTransport tt = null;
        InetSocketAddress sock_addr = null;

        StringBuilder serverAddrSequence = new StringBuilder();
        String hostport0String = hostport0.getAddress().getHostAddress() + ":" + hostport0.getPort();
        String hostport1String = hostport1.getAddress().getHostAddress() + ":" + hostport1.getPort();
        String expectedSequence1 = hostport0String + " " + hostport1String + " " + hostport0String;
        String expectedSequence2 = hostport1String + " " + hostport0String + " " + hostport1String;
        String expectedSequenceLocalMode = hostport0String + " " + hostport0String + " " + hostport0String;

        Configuration cfg = createRemoteCacheManagerConfigurationBuilder().build();

        RemoteCacheManager rcm = new RemoteCacheManager(cfg);
        RemoteCache rc = rcm.getCache(TEST_CACHE_NAME);
        RemoteCacheImpl rci = (RemoteCacheImpl) rc;

        // the factory used to create all remote operations for this class
        OperationsFactory of = getOperationsFactoryField(rci);
        TcpTransportFactory ttf = (TcpTransportFactory) getTransportFactoryField(of);

        // perform first simulated operation
        tt = (TcpTransport) ttf.getTransport(null, rci.getName().getBytes());
        sock_addr = (InetSocketAddress) tt.getServerAddress();
        ttf.releaseTransport(tt);
        serverAddrSequence.append(sock_addr.getAddress().getHostAddress() + ":" + sock_addr.getPort()).append(" ");

        tt = (TcpTransport) ttf.getTransport(null, rci.getName().getBytes());
        sock_addr = (InetSocketAddress) tt.getServerAddress();
        ttf.releaseTransport(tt);
        serverAddrSequence.append(sock_addr.getAddress().getHostAddress() + ":" + sock_addr.getPort()).append(" ");

        tt = (TcpTransport) ttf.getTransport(null, rci.getName().getBytes());
        sock_addr = (InetSocketAddress) tt.getServerAddress();
        ttf.releaseTransport(tt);
        serverAddrSequence.append(sock_addr.getAddress().getHostAddress() + ":" + sock_addr.getPort());

        if (!isLocalMode()) {
            assertTrue(
                    "loadbalancing server sequence expected either " + expectedSequence1 + " or " + expectedSequence2
                            + ", actual sequence: " + serverAddrSequence.toString(),
                    serverAddrSequence.toString().equals(expectedSequence1)
                            || serverAddrSequence.toString().equals(expectedSequence2));
        } else {
            assertEquals("LOCAL mode - loadbalancing server sequence expected " + expectedSequenceLocalMode
                    + ", actual sequence: " + serverAddrSequence.toString(),
                    serverAddrSequence.toString(), expectedSequenceLocalMode);
        }
    }

    protected static boolean isLocalMode() {
        return getClusteringMode().contains("local");
    }

    /*
     * Tests the load balancing feature
     *
     * Checks that a custom load balancing strategy, Node0Only, will cycle through the server list as operations are executed.
     *
     * NOTE: the default properties have a server list of node0/node1.
     */
    @Test
    public void testCustomLoadBalancing() throws Exception {
        if (!isLocalMode()) {
            doTestCustomLoadBalancing();
        }
    }

    private void doTestCustomLoadBalancing() throws Exception {
        // the InetSocketAddress instances which this test should be using
        InetSocketAddress hostport0 = new InetSocketAddress(getServers().get(0).getHotrodEndpoint().getInetAddress().getHostName(), getServers().get(0)
                .getHotrodEndpoint().getPort());
        InetSocketAddress hostport1 = new InetSocketAddress(getServers().get(1).getHotrodEndpoint().getInetAddress().getHostName(), getServers().get(1)
                .getHotrodEndpoint().getPort());

        TcpTransport tt = null;
        InetSocketAddress sock_addr = null;

        // create configuration with the custom balancing strategy
        Configuration cfg = createRemoteCacheManagerConfigurationBuilder()
                .balancingStrategy("org.infinispan.server.test.client.hotrod.Node0OnlyBalancingStrategy")
                .build();

        RemoteCacheManager rcm = new RemoteCacheManager(cfg);
        RemoteCache rc = rcm.getCache(TEST_CACHE_NAME);
        RemoteCacheImpl rci = (RemoteCacheImpl) rc;
        // the factory used to create all remote operations for this class
        OperationsFactory of = getOperationsFactoryField(rci);
        TcpTransportFactory ttf = (TcpTransportFactory) getTransportFactoryField(of);
        // perform first simulated operation
        tt = (TcpTransport) ttf.getTransport(null, rci.getName().getBytes());
        sock_addr = (InetSocketAddress) tt.getServerAddress();
        ttf.releaseTransport(tt);
        assertEquals("load balancing first request: server address expected " + hostport0 + ", actual server address "
                + sock_addr, sock_addr, hostport0);

        tt = (TcpTransport) ttf.getTransport(null, rci.getName().getBytes());
        sock_addr = (InetSocketAddress) tt.getServerAddress();
        ttf.releaseTransport(tt);
        assertEquals("load balancing second request: server address expected " + hostport0 + ", actual server address"
                + sock_addr, sock_addr, hostport0);
    }

    private boolean putGetFromDefaultCache(RemoteCacheManager rcm) {
        RemoteCache<String, Integer> rc = rcm.getCache();
        rc.put("testKey", 139);
        boolean success = rc.get("testKey") == 139;
        rc.remove("testKey");
        return success;
    }

    private void assertEqualConfiguration(Configuration config, RemoteCache rc) throws Exception {
        assertEquals(config.balancingStrategy().getName(), getRequestBalancingStrategyProperty(rc));

        // Configuration stores servers as List<ServerConfiguration>, getServerListProperty returns string "host1:port1;host2:port2..."
        String servers = getServerListProperty(rc);
        for (ServerConfiguration scfg : config.servers()) {
            boolean found = false;
            String host;
            int port = ConfigurationProperties.DEFAULT_HOTROD_PORT;
            Pattern patternIpv6 = Pattern.compile(IPV6_REGEX);
            Pattern patternIpv4 = Pattern.compile(IPV4_REGEX);
            for (String server : servers.split(";")) {
                Matcher matcher6 = patternIpv6.matcher(server);
                Matcher matcher4 = patternIpv4.matcher(server);
                if (matcher6.matches()) {
                    host = matcher6.group(1);
                    port = Integer.parseInt(matcher6.group(2));
                } else if (matcher4.matches()) {
                    host = matcher4.group(1);
                    port = Integer.parseInt(matcher4.group(2));
                } else {
                    host = server;
                }
                if (scfg.host().equals(host) && scfg.port() == port)
                    found = true;
            }
            if (!found)
                fail("The remote cache manager was configured to have server with an address " + scfg.host() + ":" + scfg.port() + ", but it doesn't.");
        }

        assertEquals(config.forceReturnValues(), Boolean.parseBoolean(getForceReturnValueProperty(rc)));
        assertEquals(config.tcpNoDelay(), Boolean.parseBoolean(getTcpNoDelayProperty(rc)));
        assertEquals(config.tcpKeepAlive(), Boolean.parseBoolean(getTcpKeepAliveProperty(rc)));
        assertEquals(config.maxRetries(), Integer.parseInt(getMaxRetries(rc)));

        // pingOnStartup and asyncExecutorFactory compared only with the configuration itself
        assertEquals(config.pingOnStartup(), rc.getRemoteCacheManager().getConfiguration().pingOnStartup());
        assertEquals(config.asyncExecutorFactory().factoryClass().getName(),
                rc.getRemoteCacheManager().getConfiguration().asyncExecutorFactory().factoryClass().getName());

        assertEquals(config.transportFactory().getName(), getTransportFactoryProperty(rc));
        // either marshaller or marshallerClass is set
        if (config.marshaller() != null) {
            assertEquals(config.marshaller().getClass().getName(), getMarshallerProperty(rc));
        } else {
            assertEquals(config.marshallerClass().getName(), getMarshallerProperty(rc));
        }

        // need to do some hotrod operation, only then is the hash initialized
        rc.stats();
        // get hash function only for distribution mode
        if (isDistributedMode()) {
            assertEquals(config.consistentHashImpl(3).getName(), getHashFunctionImplProperty(rc));
        }

        assertEquals(config.keySizeEstimate(), getKeySizeEstimateProperty(rc));
        assertEquals(config.valueSizeEstimate(), getValueSizeEstimateProperty(rc));
    }

    private String getRequestBalancingStrategyProperty(RemoteCache rc) throws Exception {
        RemoteCacheImpl rci = (RemoteCacheImpl) rc;
        OperationsFactory of = getOperationsFactoryField(rci);
        TcpTransportFactory ttf = (TcpTransportFactory) getTransportFactoryField(of);
        RequestBalancingStrategy rbs = ttf.getBalancer(RemoteCacheManager.cacheNameBytes());
        return rbs.getClass().getName();
    }

    private String getServerListProperty(RemoteCache rc) throws Exception {

        RemoteCacheImpl rci = (RemoteCacheImpl) rc;
        OperationsFactory of = getOperationsFactoryField(rci);
        TcpTransportFactory ttf = (TcpTransportFactory) getTransportFactoryField(of);
        Collection<SocketAddress> servers = ttf.getServers();
        // create a list of IP address:port to return
        StringBuffer serverList = new StringBuffer();
        int listSize = servers.size();
        int i = 0;
        for (Iterator iter = servers.iterator(); iter.hasNext(); i++) {
            InetSocketAddress addr = (InetSocketAddress) iter.next();
            // take care to remove prepended backslash
            String serverAddress = addr.getAddress().toString();
//            if (serverAddress.startsWith("/"))
//                serverAddress = serverAddress.substring(1);
//            serverList.append(serverAddress);
            if (addr.getAddress() instanceof Inet6Address) {
                serverList.append('[').append(addr.getHostName()).append(']');
            } else {
                serverList.append(addr.getHostName());
            }
            serverList.append(":");
            serverList.append(addr.getPort());
            if (i < listSize - 1)
                serverList.append(";");
        }
        return serverList.toString();
    }

    private String getForceReturnValueProperty(RemoteCache rc) throws Exception {

        RemoteCacheImpl rci = (RemoteCacheImpl) rc;
        OperationsFactory of = getOperationsFactoryField(rci);
        boolean forceReturn = getForceReturnValueField(of);
        return Boolean.toString(forceReturn);
    }

    private String getTcpNoDelayProperty(RemoteCache rc) throws Exception {

        RemoteCacheImpl rci = (RemoteCacheImpl) rc;
        OperationsFactory of = getOperationsFactoryField(rci);
        TcpTransportFactory ttf = (TcpTransportFactory) getTransportFactoryField(of);
        boolean tcpNoDelay = ttf.isTcpNoDelay();
        return Boolean.toString(tcpNoDelay);
    }

    private String getTcpKeepAliveProperty(RemoteCache rc) throws Exception {

        RemoteCacheImpl rci = (RemoteCacheImpl) rc;
        OperationsFactory of = getOperationsFactoryField(rci);
        TcpTransportFactory ttf = (TcpTransportFactory) getTransportFactoryField(of);
        boolean tcpKeepAlive = ttf.isTcpKeepAlive();
        return Boolean.toString(tcpKeepAlive);
    }

    private String getMaxRetries(RemoteCache rc) throws Exception {
        RemoteCacheImpl rci = (RemoteCacheImpl) rc;
        OperationsFactory of = getOperationsFactoryField(rci);
        TransportFactory ttf = getTransportFactoryField(of);
        return Integer.toString(ttf.getMaxRetries());
    }

    private String getTransportFactoryProperty(RemoteCache rc) throws Exception {

        RemoteCacheImpl rci = (RemoteCacheImpl) rc;
        OperationsFactory of = getOperationsFactoryField(rci);
        TransportFactory tf = getTransportFactoryField(of);
        // need to check instance type
        return tf.getClass().getName();
    }

    private String getMarshallerProperty(RemoteCache rc) throws Exception {

        RemoteCacheImpl rci = (RemoteCacheImpl) rc;
        Marshaller m = getMarshallerField(rci);
        // need to check instance
        return m.getClass().getName();
    }

    private String getHashFunctionImplProperty(RemoteCache rc) throws Exception {
        RemoteCacheImpl rci = (RemoteCacheImpl) rc;
        OperationsFactory of = getOperationsFactoryField(rci);
        TcpTransportFactory ttf = (TcpTransportFactory) getTransportFactoryField(of);
        ConsistentHash ch = ttf.getConsistentHash(((RemoteCacheImpl) rc).getName().getBytes());
        return ch.getClass().getName();
    }

    private int getKeySizeEstimateProperty(RemoteCache rc) throws Exception {

        RemoteCacheImpl rci = (RemoteCacheImpl) rc;
        return getEstimateKeySizeField(rci);
    }

    private int getValueSizeEstimateProperty(RemoteCache rc) throws Exception {

        RemoteCacheImpl rci = (RemoteCacheImpl) rc;
        return getEstimateValueSizeField(rci);
    }

    protected static String getClusteringMode() {
        return System.getProperty("clustering.mode", DEFAULT_CLUSTERING_MODE);
    }

    private OperationsFactory getOperationsFactoryField(RemoteCacheImpl rci) throws Exception {

        Field field = null;
        try {
            field = RemoteCacheImpl.class.getDeclaredField("operationsFactory");
        } catch (NoSuchFieldException e) {
            throw new Exception("Could not access operationsFactory field", e);
        }
        field.setAccessible(true);
        OperationsFactory fieldValue = null;
        try {
            fieldValue = (OperationsFactory) field.get(rci);
        } catch (IllegalAccessException e) {
            throw new Exception("Could not access OperationsFactory field", e);
        }
        return fieldValue;
    }

    private int getEstimateKeySizeField(RemoteCacheImpl rci) throws Exception {

        Field field = null;
        try {
            field = RemoteCacheImpl.class.getDeclaredField("estimateKeySize");
        } catch (NoSuchFieldException e) {
            throw new Exception("Could not access estimateKeySize field", e);
        }
        field.setAccessible(true);
        int fieldValue = 0;
        try {
            fieldValue = field.getInt(rci);
        } catch (IllegalAccessException e) {
            throw new Exception("Could not access estimateKeySize field", e);
        }
        return fieldValue;
    }

    private int getEstimateValueSizeField(RemoteCacheImpl rci) throws Exception {

        Field field = null;
        try {
            field = RemoteCacheImpl.class.getDeclaredField("estimateValueSize");
        } catch (NoSuchFieldException e) {
            throw new Exception("Could not access estimateValueSize field", e);
        }
        field.setAccessible(true);
        int fieldValue = 0;
        try {
            fieldValue = field.getInt(rci);
        } catch (IllegalAccessException e) {
            throw new Exception("Could not access estimateValueSize field", e);
        }
        return fieldValue;
    }

    private Marshaller getMarshallerField(RemoteCacheImpl rci) throws Exception {

        Field field = null;
        try {
            field = RemoteCacheImpl.class.getDeclaredField("marshaller");
        } catch (NoSuchFieldException e) {
            throw new Exception("Could not access marshaller field", e);
        }
        field.setAccessible(true);
        Marshaller fieldValue = null;
        try {
            fieldValue = (Marshaller) field.get(rci);
        } catch (IllegalAccessException e) {
            throw new Exception("Could not access marshaller field", e);
        }
        return fieldValue;
    }

    private boolean getForceReturnValueField(OperationsFactory of) throws Exception {

        Field field = null;
        try {
            field = OperationsFactory.class.getDeclaredField("forceReturnValue");
        } catch (NoSuchFieldException e) {
            throw new Exception("Could not access forceReturnValue field", e);
        }
        field.setAccessible(true);
        boolean fieldValue = false;
        try {
            fieldValue = field.getBoolean(of);
        } catch (IllegalAccessException e) {
            throw new Exception("Could not access forceReturnValue field", e);
        }
        return fieldValue;
    }

    private TransportFactory getTransportFactoryField(OperationsFactory of) throws Exception {

        Field field = null;
        try {
            field = OperationsFactory.class.getDeclaredField("transportFactory");
        } catch (NoSuchFieldException e) {
            throw new Exception("Could not access transportFactory field", e);
        }
        field.setAccessible(true);
        TransportFactory fieldValue = null;
        try {
            fieldValue = (TransportFactory) field.get(of);
        } catch (IllegalAccessException e) {
            throw new Exception("Could not access transportFactory field", e);
        }
        return fieldValue;
    }
}
