package org.horizon.loader.jdbc;

import org.horizon.loader.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.horizon.loader.jdbc.connectionfactory.PooledConnectionFactory;
import org.horizon.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.util.HashSet;
import java.util.Set;

/**
 * Tester class for {@link org.horizon.loader.jdbc.connectionfactory.PooledConnectionFactory}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "loader.jdbc.PooledConnectionFactoryTest", enabled = false,
      description = "This test is disabled due to: http://sourceforge.net/tracker/index.php?func=detail&aid=1892195&group_id=25357&atid=383690")
public class PooledConnectionFactoryTest {

   private PooledConnectionFactory factory;


   @AfterMethod
   public void destroyFacotry() {
      factory.stop();
   }

   public void testValuesNoOverrides() throws Exception {
      factory = new PooledConnectionFactory();
      ConnectionFactoryConfig config = UnitTestDatabaseManager.getUniqueConnectionFactoryConfig();
      factory.start(config);
      int hadcodedMaxPoolSize = factory.getPooledDataSource().getMaxPoolSize();
      Set<Connection> connections = new HashSet<Connection>();
      for (int i = 0; i < hadcodedMaxPoolSize; i++) {
         connections.add(factory.getConnection());
      }
      assert connections.size() == hadcodedMaxPoolSize;
      assert factory.getPooledDataSource().getNumBusyConnections() == hadcodedMaxPoolSize;
      for (Connection conn : connections) {
         conn.close();
      }
      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < 2000) {
         if (factory.getPooledDataSource().getNumBusyConnections() == 0) break;
      }
      //this must happen eventually 
      assert factory.getPooledDataSource().getNumBusyConnections() == 0;
   }

}
