/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.test.hibernate.cache.commons.functional;

import java.util.List;
import java.util.Map;

import org.hibernate.testing.TestForIssue;
import org.infinispan.hibernate.cache.commons.InfinispanBaseRegion;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;
import org.hibernate.stat.Statistics;
import org.infinispan.test.hibernate.cache.commons.functional.entities.Item;
import org.infinispan.test.hibernate.cache.commons.util.InducedException;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactory;
import org.infinispan.util.ControlledTimeService;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Parent tests for both transactional and
 * read-only tests are defined in this class.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
public class ReadOnlyTest extends SingleNodeTest {
	protected static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog(ReadOnlyTest.class);
	protected static final ControlledTimeService TIME_SERVICE = new ControlledTimeService();

	@Override
	public List<Object[]> getParameters() {
		return getParameters(false, false, true, true);
	}

	@Test
	public void testEmptySecondLevelCacheEntry() {
		sessionFactory().getCache().evictCollectionRegion( Item.class.getName() + ".items" );
		Statistics stats = sessionFactory().getStatistics();
		stats.clear();
		InfinispanBaseRegion region = TEST_SESSION_ACCESS.getRegion(sessionFactory(), Item.class.getName() + ".items");
		assertEquals(0, region.getElementCountInMemory());
	}

	@Test
	public void testInsertDeleteEntity() throws Exception {
		final Statistics stats = sessionFactory().getStatistics();
		stats.clear();

		final Item item = new Item( "chris", "Chris's Item" );
		withTxSession(s -> s.persist(item));

		log.info("Entry persisted, let's load and delete it.");

		withTxSession(s -> {
			Item found = s.load(Item.class, item.getId());
			log.info(stats.toString());
			assertEquals(item.getDescription(), found.getDescription());
			assertEquals(0, stats.getSecondLevelCacheMissCount());
			assertEquals(1, stats.getSecondLevelCacheHitCount());
			s.delete(found);
		});
	}

	@Test
	public void testInsertClearCacheDeleteEntity() throws Exception {
		final Statistics stats = sessionFactory().getStatistics();
		stats.clear();

		final Item item = new Item( "chris", "Chris's Item" );
		withTxSession(s -> s.persist(item));
		assertEquals(0, stats.getSecondLevelCacheMissCount());
		assertEquals(0, stats.getSecondLevelCacheHitCount());
		assertEquals(1, stats.getSecondLevelCachePutCount());

		log.info("Entry persisted, let's load and delete it.");

		cleanupCache();
		TIME_SERVICE.advance(1);

		withTxSession(s -> {
			Item found = s.load(Item.class, item.getId());
			log.info(stats.toString());
			assertEquals(item.getDescription(), found.getDescription());
			assertEquals(1, stats.getSecondLevelCacheMissCount());
			assertEquals(0, stats.getSecondLevelCacheHitCount());
			assertEquals(2, stats.getSecondLevelCachePutCount());
			s.delete(found);
		});
	}

   @Test
   @TestForIssue(jiraKey = "ISPN-9369")
   public void testCacheQueryResultsOnRollback() throws Exception {
      final Statistics stats = sessionFactory().getStatistics();
      stats.clear();

      final Item item = new Item("query-cache-rb", "Querying caching on RB");
      withTxSession(s -> s.persist(item));

      // Delay added to guarantee that query cache results won't be considered
      // as not up to date due to persist session and query results from first
      // query happening simultaneously.
      TIME_SERVICE.advance(60001);

      try {
         withTxSession(s -> {
            final List results = TEST_SESSION_ACCESS.execQueryListCacheable(s, "from Item");
            assertFalse(results.isEmpty());
            assertEquals(0, stats.getQueryCacheHitCount());
            assertEquals(1, stats.getQueryCacheMissCount());
            assertEquals(1, stats.getQueryCachePutCount());
            throw new InducedException("Force it to rollback");
         });
      } catch (InducedException e) {
         // ignore
      }

      stats.clear();

      withTxSession(s -> {
         final List results = TEST_SESSION_ACCESS.execQueryListCacheable(s, "from Item");
         assertFalse(results.isEmpty());
         assertEquals(1, stats.getQueryCacheHitCount());
         assertEquals(0, stats.getQueryCacheMissCount());
         assertEquals(0, stats.getQueryCachePutCount());
      });
   }

   @Test
   @TestForIssue(jiraKey = "ISPN-9369")
   public void testAvoidUncommittedResultsInQuery() throws Exception {
      final Statistics stats = sessionFactory().getStatistics();
      stats.clear();

      final Item item = new Item("item 1", "item one");
      withTxSession(s -> s.persist(item));

      // Delay added to guarantee that query cache results won't be considered
      // as not up to date due to persist session and query results from first
      // query happening simultaneously.
      TIME_SERVICE.advance(60001);

      withTxSession(s -> {
         final List results = TEST_SESSION_ACCESS.execQueryListCacheable(s, "from Item");
         assertEquals(1, results.size());
         assertEquals(0, stats.getQueryCacheHitCount());
         assertEquals(1, stats.getQueryCacheMissCount());
         assertEquals(1, stats.getQueryCachePutCount());
      });

      stats.clear();

      try {
         withTxSession(s -> {
            final Item item2 = new Item("item 2", "item two");
            s.persist(item2);
            final List results = TEST_SESSION_ACCESS.execQueryListCacheable(s, "from Item");
            assertEquals(2, results.size());
            assertEquals(0, stats.getQueryCacheHitCount());
            assertEquals(1, stats.getQueryCacheMissCount());
            assertEquals(1, stats.getQueryCachePutCount());
            throw new InducedException("Force it to rollback");
         });
      } catch (InducedException e) {
         // ignore
      }

      stats.clear();

      withTxSession(s -> {
         final List results = TEST_SESSION_ACCESS.execQueryListCacheable(s, "from Item");
         assertEquals(1, results.size());
         assertEquals(0, stats.getQueryCacheHitCount());
         assertEquals(1, stats.getQueryCacheMissCount());
         assertEquals(1, stats.getQueryCachePutCount());
      });
   }

	@Override
	protected void addSettings(Map settings) {
		super.addSettings(settings);
		settings.put(TestRegionFactory.TIME_SERVICE, TIME_SERVICE);
	}
}
