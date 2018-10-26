package org.infinispan.hibernate.search;

import static org.infinispan.hibernate.search.ClusterTestHelper.clusterSize;
import static org.infinispan.hibernate.search.ClusterTestHelper.createClusterNode;
import static org.infinispan.hibernate.search.ClusterTestHelper.waitMembersCount;

import static org.junit.Assert.assertEquals;

import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.hibernate.Transaction;
import org.hibernate.search.FullTextQuery;
import org.hibernate.search.FullTextSession;
import org.hibernate.search.spi.IndexedTypeIdentifier;
import org.hibernate.search.spi.IndexedTypeSet;
import org.hibernate.search.spi.impl.PojoIndexedTypeIdentifier;
import org.hibernate.search.test.util.FullTextSessionBuilder;
import org.infinispan.hibernate.search.ClusterTestHelper.ExclusiveIndexUse;
import org.infinispan.hibernate.search.ClusterTestHelper.IndexingFlushMode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * In this test we initially start a master node which will stay alive for the full test duration and constantly
 * indexing new entities, focusing on the configurtion using async indexing and exclusive lock ownership on the primary
 * node.
 * <p/>
 * After that we add and remove additional new nodes, still making more index changes checking that each node is always
 * able to see changes - although the purpose here is to test async indexing so the visibility on these changes might
 * be slightly delayed.
 * This results in a very stressfull test as the cluster topology is changed frequently, but since it uses replication
 * it doesn't need to perform rehashing.
 *
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2017 Red Hat Inc.
 */
public class AsyncLiveRunningTest {

   // Raise the following constants to run it as a stress test:

   private static final int TEST_RUNS = 7;
   private static final int CLUSTER_RESIZE_EVERY_N_OPERATIONS = 2;
   private static final int MAX_SLAVES = 3;
   private static final boolean VERBOSE = false;
   private static final IndexingFlushMode flushMode = IndexingFlushMode.ASYNC;

   private static final IndexedTypeIdentifier EMAIL_TYPE = new PojoIndexedTypeIdentifier(SimpleEmail.class);
   private static final IndexedTypeSet TEST_TYPES = EMAIL_TYPE.asTypeSet();
   private static final long TIMEOUT_ASYNCINDEX_WAIT_MS = 5000;

   private final FullTextSessionBuilder master = createClusterNode(TEST_TYPES, ExclusiveIndexUse.EXCLUSIVE, flushMode);
   private final List<FullTextSessionBuilder> slaves = new LinkedList<>();

   private boolean growCluster = true;

   private int storedEmailsCount = 0;

   @Test
   public void liveRun() {
      try {
         for (int i = 0; i < TEST_RUNS; i++) {
            writeOnMaster();
            adjustSlavesNumber(i);
            assertViews();
            printout("cycles run: " + i);
         }
      } finally {
         master.close();
         for (FullTextSessionBuilder slave : slaves) {
            slave.close();
         }
      }
   }

   private void assertViews() {
      final long failTime = System.currentTimeMillis() + TIMEOUT_ASYNCINDEX_WAIT_MS;
      assertView(master, failTime, false);
      int slaveCount = 0;
      for (FullTextSessionBuilder slave : slaves) {
         assertView(slave, failTime, (slaveCount++ == 0));
      }
   }

   private void assertView(FullTextSessionBuilder node, final long failTime, boolean printTimings) {
      assertEquals(slaves.size() + 1, clusterSize(node, EMAIL_TYPE));
      long remainingTime = 1;
      while (true) {
         if (remainingTime < 0) {
            org.junit.Assert.fail("Timeout excedded, index state still not consistent across nodes");
         }
         FullTextSession session = node.openFullTextSession();
         try {
            FullTextQuery fullTextQuery = session.createFullTextQuery(new MatchAllDocsQuery());
            int resultSize = fullTextQuery.getResultSize();
            remainingTime = failTime - System.currentTimeMillis(); //stopwatch after query execution
            if (resultSize == storedEmailsCount) {
               if (printTimings) printout("Matching data found on first slave in less than ms: " + remainingTime);
               return; //All good
            }
         } finally {
            session.close();
         }
      }
   }

   private void adjustSlavesNumber(int i) {
      if (i % CLUSTER_RESIZE_EVERY_N_OPERATIONS != 0) {
         return;
      }
      if (growCluster) {
         if (slaves.size() >= MAX_SLAVES) {
            growCluster = false;
         } else {
            slaves.add(createClusterNode(TEST_TYPES, ExclusiveIndexUse.SHARED, flushMode));
         }
      } else {
         if (slaves.size() == 0) {
            growCluster = true;
         } else {
            FullTextSessionBuilder sessionBuilder = slaves.remove(0);
            sessionBuilder.close();
         }
      }
      waitForAllJoinsCompleted();
   }

   private void writeOnMaster() {
      FullTextSession fullTextSession = master.openFullTextSession();
      try {
         Transaction transaction = fullTextSession.beginTransaction();
         SimpleEmail simpleEmail = new SimpleEmail();
         simpleEmail.to = "outher space";
         simpleEmail.message = "anybody out there?";
         fullTextSession.save(simpleEmail);
         transaction.commit();
         storedEmailsCount++;
      } finally {
         fullTextSession.close();
      }
   }

   private void waitForAllJoinsCompleted() {
      int expectedSize = slaves.size() + 1;
      waitMembersCount(master, EMAIL_TYPE, expectedSize);
      for (FullTextSessionBuilder slave : slaves) {
         waitMembersCount(slave, EMAIL_TYPE, expectedSize);
      }
   }

   private void printout(String message) {
      if (VERBOSE) {
         System.out.println(message);
      }
   }

   @BeforeClass
   public static void prepareConnectionPool() {
      ClusterSharedConnectionProvider.realStart();
   }

   @AfterClass
   public static void shutdownConnectionPool() {
      ClusterSharedConnectionProvider.realStop();
   }

}
