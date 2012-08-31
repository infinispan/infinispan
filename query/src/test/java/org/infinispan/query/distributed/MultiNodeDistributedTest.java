/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.query.distributed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.transaction.TransactionManager;

import junit.framework.Assert;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.hibernate.search.engine.spi.SearchFactoryImplementor;
import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.indexmanager.InfinispanCommandsBackend;
import org.infinispan.query.indexmanager.InfinispanIndexManager;
import org.infinispan.query.test.Person;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Configures the Hibernate Search backend to use Infinispan custom commands as a backend
 * transport, and a consistent hash for Master election for each index.
 * The test changes the view several times while indexing and verifying index state.
 * 
 * @author Sanne Grinovero
 */
@Test(groups = "functional", testName = "query.distributed.MultiNodeDistributedTest", enabled = false, description = "Temporary disabled : https://issues.jboss.org/browse/ISPN-2249")
public class MultiNodeDistributedTest extends AbstractInfinispanTest {

   private List<EmbeddedCacheManager> cacheManagers = new ArrayList<EmbeddedCacheManager>(4);
   private List<Cache<String, Person>> caches = new ArrayList<Cache<String, Person>>(4);

   protected EmbeddedCacheManager createCacheManager() throws IOException {
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.fromXml(getConfigurationResourceName());
      cacheManagers.add(cacheManager);
      Cache<String, Person> cache = cacheManager.getCache();
      caches.add(cache);
      TestingUtil.waitForRehashToComplete(caches);
      return cacheManager;
   }

   protected String getConfigurationResourceName() {
      return "dynamic-indexing-distribution.xml";
   }

   private void storeOn(Cache<String, Person> cache, String key, Person person) throws Exception {
      TransactionManager transactionManager = null;
      transactionManager = cache.getAdvancedCache().getTransactionManager();
      if (transactionsEnabled()) transactionManager.begin();
      cache.put(key, person);
      if (transactionsEnabled()) transactionManager.commit();
   }

   public void testIndexingWorkDistribution() throws Exception {
      try {
         createCacheManager();
         createCacheManager();
         assertIndexSize(0);
         //depending on test run, the index master selection might pick either cache.
         //We don't know which cache it picks, but we allow writing & searching on all.
         storeOn(caches.get(0), "k1", new Person("K. Firt", "Is not a character from the matrix", 1));
         assertIndexSize(1);
         storeOn(caches.get(1), "k2", new Person("K. Seycond", "Is a pilot", 1));
         assertIndexSize(2);
         storeOn(caches.get(0), "k3", new Person("K. Theerd", "Forgot the fundamental laws", 1));
         assertIndexSize(3);
         storeOn(caches.get(1), "k3", new Person("K. Overide", "Impersonating Mr. Theerd", 1));
         assertIndexSize(3);
         createCacheManager();
         storeOn(caches.get(2), "k4", new Person("K. Forth", "Dynamic Topology!", 1));
         assertIndexSize(4);
         createCacheManager();
         assertIndexSize(4);
         killMasterNode();
         storeOn(caches.get(2), "k5", new Person("K. Vife", "Failover!", 1));
         assertIndexSize(5);
      }
      finally {
         TestingUtil.killCacheManagers(cacheManagers);
      }
   }

   private void killMasterNode() {
      for (Cache cache : caches) {
         if (isMasterNode(cache)) {
            TestingUtil.killCacheManagers(cache.getCacheManager());
            caches.remove(cache);
            cacheManagers.remove(cache.getCacheManager());
            TestingUtil.waitForRehashToComplete(caches);
            break;
         }
      }
   }

   private boolean isMasterNode(Cache cache) {
      //Implicitly verifies the components are setup as configured by casting:
      SearchManager searchManager = Search.getSearchManager(cache);
      SearchFactoryImplementor searchFactory = (SearchFactoryImplementor) searchManager.getSearchFactory();
      InfinispanIndexManager indexManager = (InfinispanIndexManager) searchFactory.getAllIndexesManager().getIndexManager("person");
      InfinispanCommandsBackend commandsBackend = indexManager.getRemoteMaster();
      return commandsBackend.isMasterLocal();
   }

   private void assertIndexSize(int expectedIndexSize) {
      for (Cache cache : caches) {
         SearchManager searchManager = Search.getSearchManager(cache);
         CacheQuery query = searchManager.getQuery(new MatchAllDocsQuery(), Person.class);
         Assert.assertEquals(expectedIndexSize, query.list().size());
      }
   }

   private boolean transactionsEnabled() {
      return false; //TODO extend this test using a Transactional configuration
   }

}
