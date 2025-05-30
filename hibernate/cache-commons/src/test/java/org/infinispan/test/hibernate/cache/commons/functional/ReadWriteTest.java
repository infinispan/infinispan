package org.infinispan.test.hibernate.cache.commons.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.hibernate.Cache;
import org.hibernate.Hibernate;
import org.hibernate.NaturalIdLoadAccess;
import org.hibernate.Session;
import org.hibernate.cache.spi.entry.CacheEntry;
import org.hibernate.jpa.QueryHints;
import org.hibernate.query.criteria.HibernateCriteriaBuilder;
import org.hibernate.stat.CacheRegionStatistics;
import org.hibernate.stat.Statistics;
import org.hibernate.testing.TestForIssue;
import org.infinispan.commons.util.ByRef;
import org.infinispan.test.hibernate.cache.commons.functional.entities.Citizen;
import org.infinispan.test.hibernate.cache.commons.functional.entities.Citizen_;
import org.infinispan.test.hibernate.cache.commons.functional.entities.Item;
import org.infinispan.test.hibernate.cache.commons.functional.entities.NaturalIdOnManyToOne;
import org.infinispan.test.hibernate.cache.commons.functional.entities.OtherItem;
import org.infinispan.test.hibernate.cache.commons.functional.entities.State;
import org.infinispan.test.hibernate.cache.commons.functional.entities.State_;
import org.infinispan.test.hibernate.cache.commons.functional.entities.VersionedItem;
import org.junit.After;
import org.junit.Test;

import jakarta.persistence.TypedQuery;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Root;

/**
 * Functional entity transactional tests.
 *
 * @author Galder Zamarreño
 * @since 3.5
 */
public class ReadWriteTest extends ReadOnlyTest {
	@Override
	public List<Object[]> getParameters() {
		return getParameters(true, true, false, true, true);
	}

	@Override
	protected Class<?>[] getAnnotatedClasses() {
		return new Class[] {
				Citizen.class, State.class,
				NaturalIdOnManyToOne.class
		};
	}

	@After
	public void cleanupData() throws Exception {
		super.cleanupCache();
		withTxSession(s -> {
         TEST_SESSION_ACCESS.execQueryUpdate(s, "delete NaturalIdOnManyToOne");
         TEST_SESSION_ACCESS.execQueryUpdate(s, "delete Citizen");
         TEST_SESSION_ACCESS.execQueryUpdate(s, "delete State" );
		});
	}

	@Test
	public void testCollectionCache() throws Exception {
		final Statistics stats = sessionFactory().getStatistics();
		stats.clear();

		final Item item = new Item( "chris", "Chris's Item" );
		final Item another = new Item( "another", "Owned Item" );
		item.addItem( another );

		withTxSession(s -> {
			s.persist( item );
			s.persist( another );
		});
		// The collection has been removed, but we can't add it again immediately using putFromLoad
		TIME_SERVICE.advance(1);

		withTxSession(s -> {
			Item loaded = s.load( Item.class, item.getId() );
			assertEquals( 1, loaded.getItems().size() );
		});

		String itemsRegionName = Item.class.getName() + ".items";
		CacheRegionStatistics cStats = stats.getCacheRegionStatistics(itemsRegionName);
		assertEquals( 1, cStats.getElementCountInMemory() );

		withTxSession(s -> {
			Item loadedWithCachedCollection = (Item) s.load( Item.class, item.getId() );
			stats.logSummary();
			assertEquals( item.getName(), loadedWithCachedCollection.getName() );
			assertEquals( item.getItems().size(), loadedWithCachedCollection.getItems().size() );
			assertEquals( 1, cStats.getHitCount() );
			assertEquals( 1, TEST_SESSION_ACCESS.getRegion(sessionFactory(), itemsRegionName).getElementCountInMemory());
			Item itemElement = loadedWithCachedCollection.getItems().iterator().next();
			itemElement.setOwner( null );
			loadedWithCachedCollection.getItems().clear();
			s.delete( itemElement );
			s.delete( loadedWithCachedCollection );
		});
	}

	@Test
	@TestForIssue( jiraKey = "HHH-9231" )
	public void testAddNewOneToManyElementInitFlushLeaveCacheConsistent() throws Exception {
		Statistics stats = sessionFactory().getStatistics();
		stats.clear();
		CacheRegionStatistics cStats = stats.getCacheRegionStatistics(Item.class.getName() + ".items");

		ByRef<Long> itemId = new ByRef<>(null);
		saveItem(itemId);

		// create an element for item.itsms
		Item itemElement = new Item();
		itemElement.setName( "element" );
		itemElement.setDescription( "element item" );

		withTxSession(s -> {
			Item item = s.get( Item.class, itemId.get() );
			assertFalse( Hibernate.isInitialized( item.getItems() ) );
			// Add an element to item.items (a Set); it will initialize the Set.
			item.addItem( itemElement );
			assertTrue( Hibernate.isInitialized( item.getItems() ) );
			s.persist( itemElement );
			s.flush();
			markRollbackOnly(s);
		});

		withTxSession(s -> {
			Item item = s.get( Item.class, itemId.get() );
			Hibernate.initialize( item.getItems() );
			assertTrue( item.getItems().isEmpty() );
			s.delete( item );
		});
	}

	@Test
	@TestForIssue( jiraKey = "HHH-9231" )
	public void testAddNewOneToManyElementNoInitFlushLeaveCacheConsistent() throws Exception {
		Statistics stats = sessionFactory().getStatistics();
		stats.clear();
		CacheRegionStatistics cStats = stats.getCacheRegionStatistics(Item.class.getName() + ".items");

		ByRef<Long> itemId = new ByRef<>(null);

		saveItem(itemId);

		// create an element for item.bagOfItems
		Item itemElement = new Item();
		itemElement.setName( "element" );
		itemElement.setDescription( "element item" );

		withTxSession(s -> {
			Item item = s.get( Item.class, itemId.get() );
			assertFalse( Hibernate.isInitialized( item.getItems() ) );
			// Add an element to item.bagOfItems (a bag); it will not initialize the bag.
			item.addItemToBag( itemElement );
			assertFalse( Hibernate.isInitialized( item.getBagOfItems() ) );
			s.persist( itemElement );
			s.flush();
			markRollbackOnly(s);
		});

		withTxSession(s -> {
			Item item = s.get( Item.class, itemId.get() );
			Hibernate.initialize( item.getItems() );
			assertTrue( item.getItems().isEmpty() );
			s.delete( item );
		});
	}

	@Test
	public void testAddNewOneToManyElementNoInitFlushInitLeaveCacheConsistent() throws Exception {
		Statistics stats = sessionFactory().getStatistics();
		stats.clear();

		ByRef<Long> itemId = new ByRef<>(null);

		saveItem(itemId);

		// create an element for item.bagOfItems
		Item itemElement = new Item();
		itemElement.setName( "element" );
		itemElement.setDescription( "element item" );

		withTxSession(s -> {
			Item item = s.get(Item.class, itemId.get());
			assertFalse(Hibernate.isInitialized(item.getBagOfItems()));
			// Add an element to item.bagOfItems (a bag); it will not initialize the bag.
			item.addItemToBag(itemElement);
			assertFalse(Hibernate.isInitialized(item.getBagOfItems()));
			s.persist(itemElement);
			s.flush();
			// Now initialize the collection; it will contain the uncommitted itemElement.
			Hibernate.initialize(item.getBagOfItems());
			markRollbackOnly(s);
		});

		withTxSession(s -> {
			Item item = s.get(Item.class, itemId.get());
			// Because of HHH-9231, the following will fail due to ObjectNotFoundException because the
			// collection will be read from the cache and it still contains the uncommitted element,
			// which cannot be found.
			Hibernate.initialize(item.getBagOfItems());
			assertTrue(item.getBagOfItems().isEmpty());
			s.delete(item);
		});
	}

	protected void saveItem(ByRef<Long> itemId) throws Exception {
		withTxSession(s -> {
			Item item = new Item();
			item.setName( "steve" );
			item.setDescription( "steve's item" );
			s.save( item );
			itemId.set(item.getId());
		});
	}

	@Test
	public void testAddNewManyToManyPropertyRefNoInitFlushInitLeaveCacheConsistent() throws Exception {
		Statistics stats = sessionFactory().getStatistics();
		stats.clear();
		CacheRegionStatistics cStats = stats.getCacheRegionStatistics(Item.class.getName() + ".items");

		ByRef<Long> otherItemId = new ByRef<>(null);
		withTxSession(s -> {
			OtherItem otherItem = new OtherItem();
			otherItem.setName( "steve" );
			s.save( otherItem );
			otherItemId.set(otherItem.getId());
		});

		// create an element for otherItem.bagOfItems
		Item item = new Item();
		item.setName( "element" );
		item.setDescription( "element Item" );

		withTxSession(s -> {
			OtherItem otherItem = s.get( OtherItem.class, otherItemId.get() );
			assertFalse( Hibernate.isInitialized( otherItem.getBagOfItems() ) );
			// Add an element to otherItem.bagOfItems (a bag); it will not initialize the bag.
			otherItem.addItemToBag( item );
			assertFalse( Hibernate.isInitialized( otherItem.getBagOfItems() ) );
			s.persist( item );
			s.flush();
			// Now initialize the collection; it will contain the uncommitted itemElement.
			// The many-to-many uses a property-ref
			Hibernate.initialize( otherItem.getBagOfItems() );
			markRollbackOnly(s);
		});

		withTxSession(s -> {
			OtherItem otherItem = s.get( OtherItem.class, otherItemId.get() );
			// Because of HHH-9231, the following will fail due to ObjectNotFoundException because the
			// collection will be read from the cache and it still contains the uncommitted element,
			// which cannot be found.
			Hibernate.initialize( otherItem.getBagOfItems() );
			assertTrue( otherItem.getBagOfItems().isEmpty() );
			s.delete( otherItem );
		});
	}

	@Test
	public void testStaleWritesLeaveCacheConsistent() throws Exception {
		Statistics stats = sessionFactory().getStatistics();
		stats.clear();

		ByRef<VersionedItem> itemRef = new ByRef<>(null);
		withTxSession(s -> {
			VersionedItem item = new VersionedItem();
			item.setName( "steve" );
			item.setDescription( "steve's item" );
			s.save( item );
			itemRef.set(item);
		});

		final VersionedItem item = itemRef.get();
		Long initialVersion = item.getVersion();

		// manually revert the version property
		item.setVersion(item.getVersion() - 1 );

		try {
			withTxSession(s -> s.update(item));
			fail("expected stale write to fail");
		}
		catch (Exception e) {
			log.debug("Rollback was expected", e);
		}

		// check the version value in the cache...
		Object entry = getEntry(VersionedItem.class.getName(), item.getId());
		assertNotNull(entry);
		Long cachedVersionValue = (Long) ((CacheEntry) entry).getVersion();
		assertNotNull(cachedVersionValue);
		assertEquals(initialVersion.longValue(), cachedVersionValue.longValue());

		withTxSession(s -> {
			VersionedItem item2 = s.load( VersionedItem.class, item.getId() );
			s.delete( item2 );
		});
	}

	@Test
	@TestForIssue( jiraKey = "HHH-5690")
	public void testPersistEntityFlushRollbackNotInEntityCache() throws Exception {
		Statistics stats = sessionFactory().getStatistics();
		stats.clear();

		CacheRegionStatistics slcs = stats.getCacheRegionStatistics(Item.class.getName());

		ByRef<Long> itemId = new ByRef<>(null);
		withTxSession(s -> {
			Item item = new Item();
			item.setName("steve");
			item.setDescription("steve's item");
			s.persist(item);
			s.flush();
			itemId.set(item.getId());
//			assertNotNull( slcs.getEntries().get( item.getId() ) );
			markRollbackOnly(s);
		});

		// item should not be in entity cache.
		assertEquals(0, getNumberOfItems());

		withTxSession(s -> {
			Item item = s.get( Item.class, itemId.get() );
			assertNull( item );
		});
	}

	@Test
	@TestForIssue( jiraKey = "HHH-5690")
	public void testPersistEntityFlushEvictGetRollbackNotInEntityCache() throws Exception {
		Statistics stats = sessionFactory().getStatistics();
		stats.clear();
		CacheRegionStatistics slcs = stats.getCacheRegionStatistics(Item.class.getName());

		ByRef<Long> itemId = new ByRef<>(null);
		withTxSession(s -> {
			Item item = new Item();
			item.setName("steve");
			item.setDescription("steve's item");
			s.persist(item);
			s.flush();
			itemId.set(item.getId());
			// item is cached on insert.
//			assertNotNull( slcs.getEntries().get( item.getId() ) );
			s.evict(item);
			assertEquals(slcs.getHitCount(), 0);
			item = s.get(Item.class, item.getId());
			assertNotNull(item);
//			assertEquals( slcs.getHitCount(), 1 );
//			assertNotNull( slcs.getEntries().get( item.getId() ) );
			markRollbackOnly(s);
		});

		// item should not be in entity cache.
		//slcs = stats.getSecondLevelCacheStatistics( Item.class.getName() );
		assertEquals(0, getNumberOfItems());

		withTxSession(s -> {
			Item item = s.get(Item.class, itemId.get());
			assertNull(item);
		});
	}

	@Test
	public void testQueryCacheInvalidation() throws Exception {
		Statistics stats = sessionFactory().getStatistics();
		stats.clear();

		CacheRegionStatistics slcs = stats.getCacheRegionStatistics(Item.class.getName());
		sessionFactory().getCache().evictEntityData(Item.class);

		TIME_SERVICE.advance(1);

		assertEquals(0, slcs.getPutCount());
		assertEquals(0, slcs.getElementCountInMemory());
		assertEquals(0, getNumberOfItems());

		ByRef<Long> idRef = new ByRef<>(null);
		withTxSession(s -> {
			Item item = new Item();
			item.setName( "widget" );
			item.setDescription( "A really top-quality, full-featured widget." );
			s.persist( item );
			idRef.set( item.getId() );
		});

		assertEquals( 1, slcs.getPutCount() );
		assertEquals( 1, slcs.getElementCountInMemory() );
		assertEquals( 1, getNumberOfItems());

		withTxSession(s -> {
			Item item = s.get(Item.class, idRef.get());
			assertEquals(slcs.getHitCount(), 1);
			assertEquals(slcs.getMissCount(), 0);
			item.setDescription("A bog standard item");
		});

		assertEquals(slcs.getPutCount(), 2);

		CacheEntry entry = getEntry(Item.class.getName(), idRef.get());
		Serializable[] ser = entry.getDisassembledState();
		assertEquals("widget", ser[4]);
		assertEquals("A bog standard item", ser[2]);

		withTxSession(s -> {
			Item item = s.load(Item.class, idRef.get());
			s.delete(item);
		});
	}

	@Test
	public void testQueryCache() throws Exception {
		Statistics stats = sessionFactory().getStatistics();
		stats.clear();

		Item item = new Item( "chris", "Chris's Item" );

		withTxSession(s -> s.persist( item ));

		// Delay added to guarantee that query cache results won't be considered
		// as not up to date due to persist session and query results from first
		// query happening simultaneously.
		TIME_SERVICE.advance(60001);

		withTxSession(s -> TEST_SESSION_ACCESS.execQueryListCacheable(s, "from Item"));

		withTxSession(s -> {
         TEST_SESSION_ACCESS.execQueryListCacheable(s, "from Item" );
			assertEquals( 1, stats.getQueryCacheHitCount() );
         TEST_SESSION_ACCESS.execQueryUpdate(s, "delete from Item");
		});
	}

	@Test
	public void testQueryCacheHitInSameTransaction() throws Exception {
		Statistics stats = sessionFactory().getStatistics();
		stats.clear();

		Item item = new Item( "galder", "Galder's Item" );

		withTxSession(s -> s.persist( item ));

		// Delay added to guarantee that query cache results won't be considered
		// as not up to date due to persist session and query results from first
		// query happening simultaneously.
		TIME_SERVICE.advance(60001);

		withTxSession(s -> {
         TEST_SESSION_ACCESS.execQueryListCacheable(s, "from Item" );
         TEST_SESSION_ACCESS.execQueryListCacheable(s, "from Item" );
			assertEquals(1, stats.getQueryCacheHitCount());
		});

		withTxSession(s -> TEST_SESSION_ACCESS.execQueryUpdate(s, "delete from Item"));
	}

	@Test
	public void testNaturalIdCached() throws Exception {
		saveSomeCitizens();

		// Clear the cache before the transaction begins
		cleanupCache();
		TIME_SERVICE.advance(1);

		withTxSession(s -> {
			State france = ReadWriteTest.this.getState(s, "Ile de France");

			HibernateCriteriaBuilder cb = s.getCriteriaBuilder();
			CriteriaQuery<Citizen> criteria = cb.createQuery(Citizen.class);
			Root<Citizen> root = criteria.from(Citizen.class);
			criteria.where(cb.equal(root.get(Citizen_.ssn), "1234"), cb.equal(root.get(Citizen_.state), france));

			Statistics stats = sessionFactory().getStatistics();
			stats.setStatisticsEnabled(true);
			stats.clear();
			assertEquals(
					"Cache hits should be empty", 0, stats
							.getNaturalIdCacheHitCount()
			);
			TypedQuery<Citizen> typedQuery = s.createQuery(criteria)
					.setHint(QueryHints.HINT_CACHEABLE, "true");

			// first query
			List results = typedQuery
					.getResultList();
			assertEquals(1, results.size());
			assertEquals("NaturalId Cache Hits", 0, stats.getNaturalIdCacheHitCount());
			assertEquals("NaturalId Cache Misses", 0, stats.getNaturalIdCacheMissCount());
			assertEquals("NaturalId Cache Puts", 1, stats.getNaturalIdCachePutCount());
			assertEquals("NaturalId Cache Queries", 0, stats.getNaturalIdQueryExecutionCount());

			// query a second time - result should be cached in session
			typedQuery.getResultList();
			assertEquals("NaturalId Cache Hits", 0, stats.getNaturalIdCacheHitCount());
			assertEquals("NaturalId Cache Misses", 0, stats.getNaturalIdCacheMissCount());
			assertEquals("NaturalId Cache Puts", 1, stats.getNaturalIdCachePutCount());
			assertEquals("NaturalId Cache Queries", 0, stats.getNaturalIdQueryExecutionCount());

			// cleanup
			markRollbackOnly(s);
		});
	}

	@Test
	public void testNaturalIdLoaderCached() throws Exception {
		final Statistics stats = sessionFactory().getStatistics();
		stats.setStatisticsEnabled( true );
		stats.clear();

		assertEquals( "NaturalId Cache Hits", 0, stats.getNaturalIdCacheHitCount() );
		assertEquals( "NaturalId Cache Misses", 0, stats.getNaturalIdCacheMissCount() );
		assertEquals( "NaturalId Cache Puts", 0, stats.getNaturalIdCachePutCount() );
		assertEquals( "NaturalId Cache Queries", 0, stats.getNaturalIdQueryExecutionCount() );

		saveSomeCitizens();

		assertEquals( "NaturalId Cache Hits", 0, stats.getNaturalIdCacheHitCount() );
		assertEquals( "NaturalId Cache Misses", 0, stats.getNaturalIdCacheMissCount() );
		assertEquals( "NaturalId Cache Puts", 2, stats.getNaturalIdCachePutCount() );
		assertEquals( "NaturalId Cache Queries", 0, stats.getNaturalIdQueryExecutionCount() );

		//Try NaturalIdLoadAccess after insert
		final Citizen citizen = withTxSessionApply(s -> {
			State france = ReadWriteTest.this.getState(s, "Ile de France");
			NaturalIdLoadAccess<Citizen> naturalIdLoader = s.byNaturalId(Citizen.class);
			naturalIdLoader.using("ssn", "1234").using("state", france);

			//Not clearing naturalId caches, should be warm from entity loading
			stats.clear();

			// first query
			Citizen c = naturalIdLoader.load();
			assertNotNull(c);
			assertEquals("NaturalId Cache Hits", 1, stats.getNaturalIdCacheHitCount());
			assertEquals("NaturalId Cache Misses", 0, stats.getNaturalIdCacheMissCount());
			assertEquals("NaturalId Cache Puts", 0, stats.getNaturalIdCachePutCount());
			assertEquals("NaturalId Cache Queries", 0, stats.getNaturalIdQueryExecutionCount());

			// cleanup
			markRollbackOnly(s);
			return c;
		});

		// TODO: Clear caches manually via cache manager (it's faster!!)
		cleanupCache();
		TIME_SERVICE.advance(1);
		stats.setStatisticsEnabled( true );
		stats.clear();

		//Try NaturalIdLoadAccess
		withTxSession(s -> {
			// first query
			Citizen loadedCitizen = (Citizen) s.get( Citizen.class, citizen.getId() );
			assertNotNull( loadedCitizen );
			assertEquals( "NaturalId Cache Hits", 0, stats.getNaturalIdCacheHitCount() );
			assertEquals( "NaturalId Cache Misses", 0, stats.getNaturalIdCacheMissCount() );
			assertEquals( "NaturalId Cache Puts", 1, stats.getNaturalIdCachePutCount() );
			assertEquals( "NaturalId Cache Queries", 0, stats.getNaturalIdQueryExecutionCount() );

			// cleanup
			markRollbackOnly(s);
		});

		// Try NaturalIdLoadAccess after load
		withTxSession(s -> {
			State france = ReadWriteTest.this.getState(s, "Ile de France");
			NaturalIdLoadAccess naturalIdLoader = s.byNaturalId(Citizen.class);
			naturalIdLoader.using( "ssn", "1234" ).using( "state", france );

			//Not clearing naturalId caches, should be warm from entity loading
			stats.setStatisticsEnabled( true );
			stats.clear();

			// first query
			Citizen loadedCitizen = (Citizen) naturalIdLoader.load();
			assertNotNull( loadedCitizen );
			assertEquals( "NaturalId Cache Hits", 1, stats.getNaturalIdCacheHitCount() );
			assertEquals( "NaturalId Cache Misses", 0, stats.getNaturalIdCacheMissCount() );
			assertEquals( "NaturalId Cache Puts", 0, stats.getNaturalIdCachePutCount() );
			assertEquals( "NaturalId Cache Queries", 0, stats.getNaturalIdQueryExecutionCount() );

			// cleanup
			markRollbackOnly(s);
		});

	}

	@Test
	public void testEntityCacheContentsAfterEvictAll() throws Exception {
		final List<Citizen> citizens = saveSomeCitizens();

		withTxSession(s -> {
			Cache cache = s.getSessionFactory().getCache();

			Statistics stats = sessionFactory().getStatistics();
			CacheRegionStatistics slcStats = stats.getCacheRegionStatistics(Citizen.class.getName());

			assertTrue("2lc entity cache is expected to contain Citizen id = " + citizens.get(0).getId(),
					cache.containsEntity(Citizen.class, citizens.get(0).getId()));
			assertTrue("2lc entity cache is expected to contain Citizen id = " + citizens.get(1).getId(),
					cache.containsEntity(Citizen.class, citizens.get(1).getId()));
			assertEquals(2, slcStats.getPutCount());

			cache.evictAll();
			TIME_SERVICE.advance(1);

			assertEquals(0, slcStats.getElementCountInMemory());
			assertFalse("2lc entity cache is expected to not contain Citizen id = " + citizens.get(0).getId(),
					cache.containsEntity(Citizen.class, citizens.get(0).getId()));
			assertFalse("2lc entity cache is expected to not contain Citizen id = " + citizens.get(1).getId(),
					cache.containsEntity(Citizen.class, citizens.get(1).getId()));

			Citizen citizen = s.load(Citizen.class, citizens.get(0).getId());
			assertNotNull(citizen);
			assertNotNull(citizen.getFirstname()); // proxy gets resolved
			assertEquals(1, slcStats.getMissCount());

			// cleanup
			markRollbackOnly(s);
		});
	}

	@Test
	public void testMultipleEvictAll() throws Exception {
		final List<Citizen> citizens = saveSomeCitizens();

		withTxSession(s -> {
			Cache cache = s.getSessionFactory().getCache();

			cache.evictAll();
			cache.evictAll();
		});
		withTxSession(s -> {
			Cache cache = s.getSessionFactory().getCache();

			cache.evictAll();

			s.delete(s.load(Citizen.class, citizens.get(0).getId()));
			s.delete(s.load(Citizen.class, citizens.get(1).getId()));
		});
	}

	private List<Citizen> saveSomeCitizens() throws Exception {
		final Citizen c1 = new Citizen();
		c1.setFirstname( "Emmanuel" );
		c1.setLastname( "Bernard" );
		c1.setSsn( "1234" );

		final State france = new State();
		france.setName( "Ile de France" );
		c1.setState( france );

		final Citizen c2 = new Citizen();
		c2.setFirstname( "Gavin" );
		c2.setLastname( "King" );
		c2.setSsn( "000" );
		final State australia = new State();
		australia.setName( "Australia" );
		c2.setState( australia );

		withTxSession(s -> {
			s.persist( australia );
			s.persist( france );
			s.persist( c1 );
			s.persist( c2 );
		});

		List<Citizen> citizens = new ArrayList<>(2);
		citizens.add(c1);
		citizens.add(c2);
		return citizens;
	}

	private State getState(Session s, String name) {
		HibernateCriteriaBuilder cb = s.getCriteriaBuilder();
		CriteriaQuery<State> criteria = cb.createQuery(State.class);
		Root<State> root = criteria.from(State.class);
		criteria.where(cb.equal(root.get(State_.name), name));

		return s.createQuery(criteria)
				.setHint(QueryHints.HINT_CACHEABLE, "true")
				.getResultList().get(0);
	}

	private int getNumberOfItems() {
		return (int) TEST_SESSION_ACCESS.getRegion(sessionFactory(), Item.class.getName()).getElementCountInMemory();
	}

	private CacheEntry getEntry(String regionName, Long key) {
		return (CacheEntry) TEST_SESSION_ACCESS.getRegion(sessionFactory(), regionName).getCache().get(key);
	}
}
