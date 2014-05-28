package org.infinispan.server.test.util;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;

import javax.management.Attribute;
import javax.management.ObjectName;

import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.server.test.client.memcached.MemcachedClient;
import org.jboss.arquillian.container.test.api.Config;
import org.jboss.arquillian.container.test.api.ContainerController;

import static org.junit.Assert.assertTrue;

/**
 * Often repeated test code routines.
 *
 * @author Michal Linhard (mlinhard@redhat.com)
 */
public class ITestUtils {
    public static final String SERVER_DATA_DIR = System.getProperty("server1.dist") + File.separator + "standalone"
            + File.separator + "data";
    public static final String SERVER_CONFIG_DIR = System.getProperty("server1.dist") + File.separator + "standalone"
            + File.separator + "configuration";
    private static final String SERVER_CONFIG_PROPERTY = "serverConfig";
    
    public static final int SERVER1_MGMT_PORT = 9990;
    public static final int SERVER2_MGMT_PORT = 10090;
    public static final int SERVER3_MGMT_PORT = 10190;
    public static final int SERVER4_MGMT_PORT = 10290;

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

    /*
  * HotRod client automatically creates a ByteArrayKey from each key regardless of its type. This
  * ByteArrayKey is then stored in a cache/store
  */
    public static byte[] getRealKeyStored(String key, RemoteCache rc) throws Exception {
        Marshaller m = getMarshallerField((RemoteCacheImpl) rc);
        return m.objectToByteBuffer(key, 64);
    }

    private static Marshaller getMarshallerField(RemoteCacheImpl rci) throws Exception {
        Field field;
        try {
            field = RemoteCacheImpl.class.getDeclaredField("marshaller");
        } catch (NoSuchFieldException e) {
            throw new Exception("Could not access marshaller field", e);
        }
        field.setAccessible(true);
        Marshaller fieldValue;
        try {
            fieldValue = (Marshaller) field.get(rci);
        } catch (IllegalAccessException e) {
            throw new Exception("Could not access marshaller field", e);
        }
        return fieldValue;
    }

    public static void startContainer(ContainerController controller, String containerName, String config) {
        controller.start(containerName, new Config().add(SERVER_CONFIG_PROPERTY, config).map());
    }

    public static void stopContainers(ContainerController controller, String... containerNames) {
        for (String name : containerNames) {
            controller.stop(name);
        }
    }

    public static RemoteInfinispanMBeans createMBeans(RemoteInfinispanServer server, String containerName, String cacheName, String managerName) {
        return RemoteInfinispanMBeans.create(server, containerName, cacheName, managerName);
    }

    public static MemcachedClient createMemcachedClient(RemoteInfinispanServer server) {
        MemcachedClient mc;
        try {
            mc = new MemcachedClient(server.getMemcachedEndpoint().getInetAddress().getHostName(), server.getMemcachedEndpoint()
                    .getPort());
        } catch (IOException e) {
            throw new RuntimeException("Could not create Memcached Client");
        }
        return mc;
    }

    public static void sleepForSecs(double numSecs) {
        // give the elements time to be evicted
        try {
            Thread.sleep((long) (numSecs * 1000));
        } catch (InterruptedException e) {
        }
    }
}
