package org.infinispan.server.test.cs.jdbc;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
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
public abstract class AbstractJdbcStoreMultinodeIT {
    private static final Log log = LogFactory.getLog(AbstractJdbcStoreMultinodeIT.class);

    protected final String CONTAINER1 = "jdbc-cachestore-1";
    protected final String CONTAINER2 = "jdbc-cachestore-2";

    private final String ID_COLUMN_NAME = "id"; // set in CacheContainerAdd in ISPN subsystem
    private final String DATA_COLUMN_NAME = "datum"; // set in CacheContainerAdd in ISPN subsystem

    protected static DBServer dbServer1 = DBServer.create();
    protected static DBServer dbServer2 = DBServer.create();

    private static RemoteCacheManagerFactory rcmFactory;
    protected RemoteCache cache;

    protected RemoteInfinispanMBeans mbeans1;
    protected RemoteInfinispanMBeans mbeans2;

    protected MemcachedClient mc1;
    protected MemcachedClient mc2;

    @ArquillianResource
    protected ContainerController controller; // being used to start and stop server manually

    @InfinispanResource(CONTAINER1)
    protected RemoteInfinispanServer server1;

    @InfinispanResource(CONTAINER2)
    protected RemoteInfinispanServer server2;

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
            if (dbServer1.bucketTable != null && dbServer1.bucketTable.getConnectionUrl().contains("db2")) {
                dbServer1.bucketTable.dropTable();
                dbServer2.bucketTable.dropTable();
            }
            if (dbServer1.stringTable != null && dbServer1.stringTable.getConnectionUrl().contains("db2")) {
                dbServer1.stringTable.dropTable();
                dbServer2.stringTable.dropTable();
            }
        } catch (Exception e) {
            // catching the exception, because the drop is not part of the tests
            log.error("Couldn't drop the tables: ", e);
        }

        if (rcmFactory != null) {
            rcmFactory.stopManagers();
        }
        rcmFactory = null;
    }

    @Before
    public void setUp() throws Exception {
        String driver = System.getProperty("driver.class");
        dBServers();

        if (dbServer1.bucketTableName != null)
            dbServer1.bucketTable = new DBServer.TableManipulation(driver, dbServer1, dbServer1.bucketTableName, ID_COLUMN_NAME, DATA_COLUMN_NAME);
        if (dbServer1.stringTableName != null)
            dbServer1.stringTable = new DBServer.TableManipulation(driver, dbServer1, dbServer1.stringTableName, ID_COLUMN_NAME, DATA_COLUMN_NAME);

        if (dbServer2.bucketTableName != null)
            dbServer2.bucketTable = new DBServer.TableManipulation(driver, dbServer2, dbServer2.bucketTableName, ID_COLUMN_NAME, DATA_COLUMN_NAME);
        if (dbServer2.stringTableName != null)
            dbServer2.stringTable = new DBServer.TableManipulation(driver, dbServer2, dbServer2.stringTableName, ID_COLUMN_NAME, DATA_COLUMN_NAME);

        mbeans1 = createMBeans(server1, CONTAINER1, cacheName(), managerName());
        mbeans2 = createMBeans(server2, CONTAINER2, cacheName(), managerName());
    }

    protected RemoteCache<Object, Object> createCache(RemoteInfinispanMBeans mbeans) {
        return rcmFactory.createCache(mbeans);
    }

    protected abstract void dBServers();

    protected abstract String managerName();

    protected abstract String cacheName();
}
