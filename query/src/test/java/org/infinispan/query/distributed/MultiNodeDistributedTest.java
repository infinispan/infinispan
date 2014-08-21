package org.infinispan.query.distributed;

import static org.junit.Assert.assertEquals;

import javax.transaction.TransactionManager;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.hibernate.search.engine.spi.SearchFactoryImplementor;
import org.infinispan.Cache;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.helper.TestableCluster;
import org.infinispan.query.indexmanager.InfinispanIndexManager;
import org.infinispan.query.test.Person;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

/**
 * Configures the Hibernate Search backend to use Infinispan custom commands as a backend
 * transport, and a consistent hash for Master election for each index.
 * The test changes the view several times while indexing and verifying index state.
 *
 * @author Sanne Grinovero
 */
@Test(groups = "functional", testName = "query.distributed.MultiNodeDistributedTest")
public class MultiNodeDistributedTest extends AbstractInfinispanTest {

   protected final TestableCluster<String,Person> cluster = new TestableCluster<>(getConfigurationResourceName());

   protected String getConfigurationResourceName() {
      return "dynamic-indexing-distribution.xml";
   }

   protected void storeOn(Cache<String, Person> cache, String key, Person person) throws Exception {
      TransactionManager transactionManager = cache.getAdvancedCache().getTransactionManager();
      if (transactionsEnabled()) transactionManager.begin();
      cache.put(key, person);
      if (transactionsEnabled()) transactionManager.commit();
   }

   public void testIndexingWorkDistribution() throws Exception {
      try {
         cluster.startNewNode();
         cluster.startNewNode();
         assertIndexSize(0);
         //depending on test run, the index master selection might pick either cache.
         //We don't know which cache it picks, but we allow writing & searching on all.
         storeOn(cluster.getCache(0), "k1", new Person("K. Firt", "Is not a character from the matrix", 1));
         assertIndexSize(1);
         storeOn(cluster.getCache(1), "k2", new Person("K. Seycond", "Is a pilot", 1));
         assertIndexSize(2);
         storeOn(cluster.getCache(0), "k3", new Person("K. Theerd", "Forgot the fundamental laws", 1));
         assertIndexSize(3);
         storeOn(cluster.getCache(1), "k3", new Person("K. Overide", "Impersonating Mr. Theerd", 1));
         assertIndexSize(3);
         cluster.startNewNode();
         storeOn(cluster.getCache(2), "k4", new Person("K. Forth", "Dynamic Topology!", 1));
         assertIndexSize(4);
         cluster.startNewNode();
         assertIndexSize(4);
         killMasterNode();
         //After a node kill, a stale lock might not be immediately resolved.
         //Solicit resolution by issues at least three writes:
         storeOn(cluster.getCache(2), "k5", new Person("K. Vife", "Gets stuck in a buffer", 1));
         storeOn(cluster.getCache(2), "k6", new Person("K. Seix", "Fills the buffer", 1));
         storeOn(cluster.getCache(2), "k7", new Person("K. Vife", "Failover!", 1));
         assertIndexSize(7);
      }
      finally {
         cluster.killAll();
      }
   }

   protected void killMasterNode() {
      for (Cache<String,Person> cache : cluster.iterateAllCaches()) {
         if (isMasterNode(cache)) {
            cluster.killNode(cache);
            break;
         }
      }
   }

   private boolean isMasterNode(Cache<?,?> cache) {
      //Implicitly verifies the components are setup as configured by casting:
      SearchManager searchManager = Search.getSearchManager(cache);
      SearchFactoryImplementor searchFactory = (SearchFactoryImplementor) searchManager.getSearchFactory();
      InfinispanIndexManager indexManager = (InfinispanIndexManager) searchFactory.getIndexManagerHolder().getIndexManager("person");
      return indexManager.isMasterLocal();
   }

   protected void assertIndexSize(int expectedIndexSize) {
      for (Cache cache : cluster.iterateAllCaches()) {
         StaticTestingErrorHandler.assertAllGood(cache);
         SearchManager searchManager = Search.getSearchManager(cache);
         CacheQuery query = searchManager.getQuery(new MatchAllDocsQuery(), Person.class);
         assertEquals(expectedIndexSize, query.list().size());
      }
   }

   protected boolean transactionsEnabled() {
      return false;
   }

}
