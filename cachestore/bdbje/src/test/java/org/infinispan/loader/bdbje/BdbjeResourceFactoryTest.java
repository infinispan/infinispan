package org.infinispan.loader.bdbje;

import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import static org.easymock.classextension.EasyMock.createMock;
import org.infinispan.loader.CacheLoaderException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Adrian Cole
 * @version $Id$
 * @since 4.0
 */
@Test(groups = "unit", enabled = true, testName = "loader.bdbje.BdbjeResourceFactoryTest")
public class BdbjeResourceFactoryTest {
   private BdbjeCacheStoreConfig cfg;
   private Environment env;
   private BdbjeResourceFactory factory;
   private Database cacheDb;
   private StoredClassCatalog catalog;

   @BeforeMethod
   public void setUp() throws Exception {
      cfg = new BdbjeCacheStoreConfig();
      factory = new BdbjeResourceFactory(cfg);
      env = createMock(Environment.class);
      cacheDb = createMock(Database.class);
      catalog = createMock(StoredClassCatalog.class);
   }

   @AfterMethod
   public void tearDown() throws CacheLoaderException {
      env = null;
      factory = null;
      cfg = null;
      cacheDb = null;
      catalog = null;
   }

   @Test(expectedExceptions = DatabaseException.class)
   public void testCreateStoredMapViewOfDatabaseThrowsException() throws DatabaseException {
      factory.createStoredMapViewOfDatabase(cacheDb, catalog);
   }
}
