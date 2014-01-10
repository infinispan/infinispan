package org.infinispan.server.test.util;

import static org.junit.Assert.assertTrue;

import java.io.File;

import javax.management.Attribute;
import javax.management.ObjectName;

import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;

/**
 * Often repeated test code routines.
 *
 * @author Michal Linhard (mlinhard@redhat.com)
 */
public class TestUtil {
    public static final String SERVER_DATA_DIR = System.getProperty("server1.dist") + File.separator + "standalone"
            + File.separator + "data";
    public static final String SERVER_CONFIG_DIR = System.getProperty("server1.dist") + File.separator + "standalone"
            + File.separator + "configuration";

    /**
     * Create {@link RemoteCacheManager} for given server.
     *
     * @param server The server
     * @return New {@link RemoteCacheManager}
     */
    public static RemoteCacheManager createCacheManager(RemoteInfinispanServer server) {
        return createCacheManager(server, ConfigurationProperties.DEFAULT_PROTOCOL_VERSION);
    }

    public static RemoteCacheManager createCacheManager(RemoteInfinispanServer server, String protocolVersion) {
        return new RemoteCacheManager(createConfigBuilder(server.getHotrodEndpoint().getInetAddress().getHostName(),
                server.getHotrodEndpoint().getPort(), protocolVersion).build());
    }

    public static ConfigurationBuilder createConfigBuilder(String hostName, int port) {
        return createConfigBuilder(hostName, port, ConfigurationProperties.DEFAULT_PROTOCOL_VERSION);
    }

    public static ConfigurationBuilder createConfigBuilder(String hostName, int port, String protocolVersion) {
        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.addServer().host(hostName).port(port).protocolVersion(protocolVersion);
        return builder;
    }

    /**
     * Create cache manager for given {@link RemoteInfinispanMBeans}.
     *
     * @param serverBeans The server MBeans.
     * @return New {@link RemoteCacheManager}
     */
    public static RemoteCacheManager createCacheManager(RemoteInfinispanMBeans serverBeans) {
        return createCacheManager(serverBeans.server);
    }

    public static String getAttribute(MBeanServerConnectionProvider provider, String mbean, String attr) throws Exception {
        return provider.getConnection().getAttribute(new ObjectName(mbean), attr).toString();
    }

    public static void setAttribute(MBeanServerConnectionProvider provider, String mbean, String attrName, Object attrValue) throws Exception {
        provider.getConnection().setAttribute(new ObjectName(mbean), new Attribute(attrName, attrValue));
    }

    public static Object invokeOperation(MBeanServerConnectionProvider provider, String mbean, String operationName, Object[] params,
                                         String[] signature) throws Exception {
        return provider.getConnection().invoke(new ObjectName(mbean), operationName, params, signature);
    }

    public interface Condition {
        public boolean isSatisfied() throws Exception;
    }

    public static void eventually(Condition ec, long timeout) {
        eventually(ec, timeout, 10);
    }

    public static void eventually(Condition ec, long timeout, int loops) {
        if (loops <= 0) {
            throw new IllegalArgumentException("Number of loops must be positive");
        }
        long sleepDuration = timeout / loops;
        if (sleepDuration == 0) {
            sleepDuration = 1;
        }
        try {
            for (int i = 0; i < loops; i++) {

                if (ec.isSatisfied())
                    return;
                Thread.sleep(sleepDuration);
            }
            assertTrue(ec.isSatisfied());
        } catch (Exception e) {
            throw new RuntimeException("Unexpected!", e);
        }
    }

}
