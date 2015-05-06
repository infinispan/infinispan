package org.infinispan.hibernate.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.hibernate.Transaction;
import org.hibernate.cfg.Environment;
import org.hibernate.search.FullTextQuery;
import org.hibernate.search.FullTextSession;
import org.hibernate.search.test.util.FullTextSessionBuilder;
import org.hibernate.search.testsupport.TestConstants;
import org.hibernate.search.util.impl.FileHelper;
import org.infinispan.hibernate.search.impl.DefaultCacheManagerService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;

/**
 * Verifies we're able to start from an existing index in Infinispan, stored in a CacheLoader. Requires a persistent
 * database so that we can shutdown the SessionFactory and start over again (simulated via a custom H2 service)
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2012 Red Hat Inc.
 */
public class StoredIndexTest {

   private FullTextSessionBuilder node;

   @Test
   public void testRestartingNode() {
      // Run 1 of application:
      startNode(true);
      try {
         storeEmail("there are some problems on this planet!");
         assertEmailsFound("some", 1);
      } finally {
         //shutdown
         stopNode();
      }

      // Restart same application:
      startNode(false);
      try {
         assertEmailsFound("some", 1);
         storeEmail("stored stuff should not vanish on this planet");
         assertEmailsFound("stuff", 1);
         assertEmailsFound("some", 1);
         assertEmailsFound("planet", 2);
      } finally {
         cleanupStoredIndex();
         stopNode();
      }
   }

   /**
    * Verifies a query on a specific term returns an expected amount of results. We do actually load entities from
    * database, so both database and index are tested.
    *
    * @param termMatch
    * @param expectedMatches
    */
   private void assertEmailsFound(String termMatch, int expectedMatches) {
      FullTextSession fullTextSession = node.openFullTextSession();
      try {
         TermQuery termQuery = new TermQuery(new Term("message", termMatch));
         FullTextQuery fullTextQuery = fullTextSession.createFullTextQuery(termQuery, SimpleEmail.class);
         List<SimpleEmail> list = fullTextQuery.list();
         Assert.assertEquals(expectedMatches, list.size());
         if (expectedMatches != 0) {
            Assert.assertEquals("complaints-office@world.com", list.get(0).to);
         }
      } finally {
         fullTextSession.close();
      }
   }

   /**
    * Saves a new test email
    */
   private void storeEmail(String content) {
      SimpleEmail email = new SimpleEmail();
      email.to = "complaints-office@world.com";
      email.message = content;
      FullTextSession fullTextSession = node.openFullTextSession();
      Transaction transaction = fullTextSession.beginTransaction();
      fullTextSession.save(email);
      transaction.commit();
      fullTextSession.close();
   }

   /**
    * Creates a new SessionFactory using a shared H2 connection pool, and running an Infinispan Directory storing the
    * index in memory and write-through filesystem.
    *
    * @param createSchema set to false to not drop an existing schema
    */
   private void startNode(boolean createSchema) {
      node = new FullTextSessionBuilder()
            .setProperty("hibernate.search.default.directory_provider", "infinispan")
            .setProperty(DefaultCacheManagerService.INFINISPAN_CONFIGURATION_RESOURCENAME, "filesystem-loading-infinispan.xml")
                  // avoid killing the schema when you still have to run the second node:
            .setProperty(Environment.HBM2DDL_AUTO, createSchema ? "create" : "validate")
                  // share the same in-memory database connection pool
            .setProperty(
                  Environment.CONNECTION_PROVIDER,
                  org.infinispan.hibernate.search.ClusterSharedConnectionProvider.class.getName()
            )
            .addAnnotatedClass(SimpleEmail.class)
            .build();
   }

   /**
    * Closes the SessionFactory, SearchFactory and Infinispan CacheManagers. Only service to survive is the H2 in memory
    * database, and the data stored by the CacheLoader enabled in the Infinispan configuration.
    */
   public void stopNode() {
      if (node != null) {
         node.close();
         node = null;
      }
   }

   /**
    * This test uses and Infinispan CacheLoader writing in $tmp directory. Make sure we at least clear the index so that
    * subsequent runs of the same test won't fail.
    */
   private void cleanupStoredIndex() {
      FullTextSession fullTextSession = node.openFullTextSession();
      try {
         Transaction transaction = fullTextSession.beginTransaction();
         fullTextSession.purgeAll(SimpleEmail.class);
         transaction.commit();
      } finally {
         fullTextSession.close();
      }
   }

   /**
    * We need to use the custom H2 connector pool to make sure that when shutting down the first node we don't kill the
    * database with all data we need for the second phase of the test.
    */
   @BeforeClass
   public static void prepareConnectionPool() {
      ClusterSharedConnectionProvider.realStart();
   }

   /**
    * Kills the static connection pool of H2 started by {@link #prepareConnectionPool()}
    */
   @AfterClass
   public static void shutdownConnectionPool() {
      ClusterSharedConnectionProvider.realStop();
   }

   /**
    * The test configuration for Infinispan is setup to offload indexes in java.io.tmpdir: clean them up. This is
    * particularly important when changing Infinispan versions as the binary format is not necessarily compatible across
    * releases.
    */
   @AfterClass
   public static void removeFileSystemStoredIndexes() {
      File targetDir = TestConstants.getTargetDir(StoredIndexTest.class);
      FileHelper.delete(new File(targetDir, "LuceneIndexesData"));
      FileHelper.delete(new File(targetDir, "LuceneIndexesMetaData"));
   }

}
