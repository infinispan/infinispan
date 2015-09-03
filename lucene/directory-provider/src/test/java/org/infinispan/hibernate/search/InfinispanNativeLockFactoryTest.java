package org.infinispan.hibernate.search;

import org.hibernate.search.cfg.spi.DirectoryProviderService;
import org.hibernate.search.indexes.spi.IndexManager;
import org.hibernate.search.testsupport.junit.SearchFactoryHolder;
import org.hibernate.search.util.impl.FileHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertNotNull;

/**
 * Verifies that the infinispan directory works with a file based lock factory
 *
 * @author gustavonalle
 */
public class InfinispanNativeLockFactoryTest {

   private static File indexBase;

   @BeforeClass
   public static void setup() throws IOException {
      indexBase = new File(System.getProperty("java.io.tmpdir"), "index");
      indexBase.mkdir();
   }

   @AfterClass
   public static void tearDown() throws IOException {
      FileHelper.delete(indexBase);
   }

   @Rule
   public SearchFactoryHolder holder = new SearchFactoryHolder(InfinispanLockFactoryOptionsTest.BookTypeZero.class)
         .withProperty("hibernate.search.default.directory_provider", "infinispan")
         .withProperty("hibernate.search.default.locking_strategy", "native")
         .withProperty("hibernate.search.infinispan.configuration_resourcename", "localonly-infinispan.xml")
         .withProperty("hibernate.search.default.indexBase", indexBase.getAbsolutePath());


   @Test
   public void verifyLockCreated() {
      IndexManager indexManager = holder.getSearchFactory().getIndexManagerHolder().getIndexManager("INDEX0");
      assertNotNull(indexManager);
   }

}
