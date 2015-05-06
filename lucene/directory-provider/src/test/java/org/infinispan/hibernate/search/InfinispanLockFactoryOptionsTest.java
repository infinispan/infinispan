package org.infinispan.hibernate.search;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.hibernate.search.annotations.DocumentId;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.cfg.spi.DirectoryProviderService;
import org.hibernate.search.indexes.spi.DirectoryBasedIndexManager;
import org.hibernate.search.indexes.spi.IndexManager;
import org.hibernate.search.store.DirectoryProvider;
import org.hibernate.search.test.directoryProvider.CustomLockFactoryProvider;
import org.hibernate.search.testsupport.junit.SearchFactoryHolder;
import org.infinispan.hibernate.search.spi.InfinispanDirectoryProvider;
import org.infinispan.lucene.locking.BaseLockFactory;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

/**
 * Verifies the locking_strategy option is being applied as expected, even if the DirectoryProvider is set to
 * Infinispan.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2014 Red Hat Inc.
 */
public class InfinispanLockFactoryOptionsTest {

   @Rule
   public SearchFactoryHolder holder = new SearchFactoryHolder(
         BookTypeZero.class, BookTypeOne.class, BookTypeTwo.class, BookTypeThree.class, BookTypeFour.class)
         .withProperty("hibernate.search.default.directory_provider", "infinispan")
         .withProperty("hibernate.search.infinispan.configuration_resourcename", "localonly-infinispan.xml")
         .withProperty("hibernate.search.INDEX1.locking_strategy", "none")
         .withProperty("hibernate.search.INDEX2.locking_strategy", CustomLockFactoryProvider.class.getName())
         .withProperty("hibernate.search.INDEX3.locking_strategy", "single");

   @Test
   public void verifyDefaulInfinispanLock() {
      verifyLockFactoryForIndexIs("INDEX0", BaseLockFactory.class);
   }

   @Test
   public void verifyNoLocking() {
      verifyLockFactoryForIndexIs("INDEX1", NoLockFactory.class);
   }

   @Test
   public void verifyCustomLocking() {
      verifyLockFactoryForIndexIs("INDEX2", SingleInstanceLockFactory.class); //as built by the CustomLockFactoryProvider
   }

   @Test
   public void verifyExplicitSingle() {
      verifyLockFactoryForIndexIs("INDEX3", SingleInstanceLockFactory.class);
   }

   @Test
   public void verifyDefaultIsInherited() {
      verifyLockFactoryForIndexIs("INDEX4", BaseLockFactory.class);
   }

   private void verifyLockFactoryForIndexIs(String indexName, Class<? extends LockFactory> expectedType) {
      Directory directory = directoryByName(indexName);
      LockFactory lockFactory = directory.getLockFactory();
      Assert.assertEquals(expectedType, lockFactory.getClass());
   }

   private Directory directoryByName(String indexName) {
      IndexManager indexManager = holder.getSearchFactory()
            .getIndexManagerHolder()
            .getIndexManager(indexName);
      Assert.assertNotNull(indexManager);
      DirectoryBasedIndexManager dpIm = (DirectoryBasedIndexManager) indexManager;
      DirectoryProvider directoryProvider = dpIm.getDirectoryProvider();
      Assert.assertNotNull(directoryProvider);
      Assert.assertTrue("Isn't an Infinispan Directory!", directoryProvider instanceof InfinispanDirectoryProvider);
      return dpIm.getDirectoryProvider().getDirectory();
   }

   @Indexed(index = "INDEX0")
   public static class BookTypeZero {
      @DocumentId
      int id;
      @Field
      String title;
   }

   @Indexed(index = "INDEX1")
   public static class BookTypeOne extends BookTypeZero {
   }

   @Indexed(index = "INDEX2")
   public static class BookTypeTwo extends BookTypeZero {
   }

   @Indexed(index = "INDEX3")
   public static class BookTypeThree extends BookTypeZero {
   }

   @Indexed(index = "INDEX4")
   public static class BookTypeFour extends BookTypeZero {
   }

}
