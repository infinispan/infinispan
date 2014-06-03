package org.infinispan.server.test.cs.jdbc;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.server.test.client.memcached.MemcachedClient;
import org.infinispan.server.test.util.jdbc.DBServer;
import org.infinispan.server.test.util.RemoteCacheManagerFactory;
import org.infinispan.server.test.util.RemoteInfinispanMBeans;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

import static org.infinispan.server.test.util.ITestUtils.createMBeans;

/**
 * @author <a href="mailto:jmarkos@redhat.com">Jakub Markos</a>
 * @author <a href="mailto:vchepeli@redhat.com">Vitalii Chepeliuk</a>
 * @since 7.0
 */
@RunWith(Arquillian.class)
public abstract class AbstractJdbcStoreSinglenodeIT {

    protected final String CONTAINER = "jdbc-cachestore-1";

    private final String ID_COLUMN_NAME = "id"; // set in CacheContainerAdd in ISPN subsystem
    private final String DATA_COLUMN_NAME = "datum"; // set in CacheContainerAdd in ISPN subsystem

    protected static DBServer dbServer = DBServer.create();

    private static RemoteCacheManagerFactory rcmFactory;
    protected RemoteCache cache;

    protected RemoteInfinispanMBeans mbeans;

    protected MemcachedClient mc;

    @ArquillianResource
    protected ContainerController controller; // being used to start and stop server manually

    @InfinispanResource(CONTAINER)
    protected RemoteInfinispanServer server;

    @BeforeClass
    public static void startup() {
        rcmFactory = new RemoteCacheManagerFactory();
    }

    @AfterClass
    public static void cleanup() {
        /**
         * We need to drop the tables, because of DB2 SQL Error: SQLCODE=-204, SQLSTATE=42704
         */
        try {
            if (dbServer.bucketTable != null && dbServer.bucketTable.getConnectionUrl().contains("db2")) {
                dbServer.bucketTable.dropTable();
                dbServer.stringTable.dropTable();
            }
        } catch (Exception e) {
            // catching the exception, because the drop is not part of the tests
            System.out.println("Couldn't drop the tables: ");
            e.printStackTrace();
        }

        if (rcmFactory != null) {
            rcmFactory.stopManagers();
        }
        rcmFactory = null;
    }

    @Before
    public void setUp() throws Exception {
        dbServer.connectionUrl = System.getProperty("connection.url");
        dbServer.username = System.getProperty("username");
        dbServer.password = System.getProperty("password");
        dbServer.bucketTableName = bucketTableName();
        dbServer.stringTableName = stringTableName();

        String driver = System.getProperty("driver.class");

        if (dbServer.bucketTableName != null)
            dbServer.bucketTable = new DBServer.TableManipulation(driver, dbServer, dbServer.bucketTableName, ID_COLUMN_NAME, DATA_COLUMN_NAME);
        if (dbServer.stringTableName != null)
            dbServer.stringTable = new DBServer.TableManipulation(driver, dbServer, dbServer.stringTableName, ID_COLUMN_NAME, DATA_COLUMN_NAME);

        mbeans = createMBeans(server, CONTAINER, cacheName(), managerName());
    }

    protected RemoteCache<Object, Object> createCache(RemoteInfinispanMBeans mbeans) {
        return rcmFactory.createCache(mbeans);
    }

    protected abstract String bucketTableName();

    protected abstract String stringTableName();

    protected abstract String managerName();

    protected abstract String cacheName();
}
