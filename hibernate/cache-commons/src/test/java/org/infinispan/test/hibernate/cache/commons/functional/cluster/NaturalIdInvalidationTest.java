package org.infinispan.test.hibernate.cache.commons.functional.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.jpa.QueryHints;
import org.hibernate.query.criteria.HibernateCriteriaBuilder;
import org.hibernate.query.criteria.JpaCriteriaQuery;
import org.infinispan.Cache;
import org.infinispan.commons.test.categories.Smoke;
import org.infinispan.hibernate.cache.commons.InfinispanBaseRegion;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;
import org.infinispan.manager.CacheContainer;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryVisitedEvent;
import org.infinispan.test.hibernate.cache.commons.functional.entities.Citizen;
import org.infinispan.test.hibernate.cache.commons.functional.entities.Citizen_;
import org.infinispan.test.hibernate.cache.commons.functional.entities.NaturalIdOnManyToOne;
import org.infinispan.test.hibernate.cache.commons.functional.entities.State;
import org.infinispan.test.hibernate.cache.commons.functional.entities.State_;
import org.infinispan.test.hibernate.cache.commons.util.TestSessionAccess;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Root;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 */
@Category(Smoke.class)
public class NaturalIdInvalidationTest extends DualNodeTest {

	private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog(NaturalIdInvalidationTest.class);

   protected static final TestSessionAccess TEST_SESSION_ACCESS = TestSessionAccess.findTestSessionAccess();

	@Rule
   public TestName name = new TestName();

	@Override
	public List<Object[]> getParameters() {
		return getParameters(true, true, true, true, true);
	}

	@Override
	protected Class<?>[] getAnnotatedClasses() {
		return new Class[] {
				Citizen.class, State.class,
				NaturalIdOnManyToOne.class
		};
	}

	@Test
	public void testAll() throws Exception {
      log.infof("*** %s", name.getMethodName());

		// Bind a listener to the "local" cache
		// Our region factory makes its CacheManager available to us
		CacheContainer localManager = ClusterAware.getCacheManager(DualNodeTest.LOCAL);
		Cache localNaturalIdCache = localManager.getCache(Citizen.class.getName() + "##NaturalId");
		MyListener localListener = new MyListener( "local" );
		localNaturalIdCache.addListener(localListener);

		// Bind a listener to the "remote" cache
		CacheContainer remoteManager = ClusterAware.getCacheManager(DualNodeTest.REMOTE);
		Cache remoteNaturalIdCache = remoteManager.getCache(Citizen.class.getName() + "##NaturalId");
		MyListener remoteListener = new MyListener( "remote" );
		remoteNaturalIdCache.addListener(remoteListener);

		SessionFactoryImplementor localFactory = sessionFactory();
		InfinispanBaseRegion localNaturalIdRegion = TEST_SESSION_ACCESS.getRegion(localFactory, Citizen.class.getName() + "##NaturalId");
		SessionFactory remoteFactory = secondNodeEnvironment().getSessionFactory();

		try {
			assertTrue(remoteListener.isEmpty());
			assertTrue(localListener.isEmpty());

			CountDownLatch remoteUpdateLatch = getRemoteUpdateLatch(remoteNaturalIdCache);
			saveSomeCitizens(localFactory);

			assertTrue(await(remoteUpdateLatch));

			assertTrue(remoteListener.isEmpty());
			assertTrue(localListener.isEmpty());

			log.debug("Find node 0");
			// This actually brings the collection into the cache
			getCitizenWithCriteria(localFactory);

			// Now the collection is in the cache so, the 2nd "get"
			// should read everything from the cache
			log.debug( "Find(2) node 0" );
			localListener.clear();
			getCitizenWithCriteria(localFactory);

			// Check the read came from the cache
			log.debug( "Check cache 0" );
			assertLoadedFromCache(localListener, "1234");

			log.debug( "Find node 1" );
			// This actually brings the collection into the cache since invalidation is in use
			getCitizenWithCriteria(remoteFactory);

			// Now the collection is in the cache so, the 2nd "get"
			// should read everything from the cache
			log.debug( "Find(2) node 1" );
			remoteListener.clear();
			getCitizenWithCriteria(remoteFactory);

			// Check the read came from the cache
			log.debug( "Check cache 1" );
			assertLoadedFromCache(remoteListener, "1234");

			// Modify customer in remote
			remoteListener.clear();
			CountDownLatch localUpdate = expectEvict(localNaturalIdCache.getAdvancedCache(), 1);
			deleteCitizenWithCriteria(remoteFactory);
			assertTrue(localUpdate.await(2, TimeUnit.SECONDS));

			assertEquals(1, localNaturalIdRegion.getElementCountInMemory());
		}
		catch (Exception e) {
			log.error("Error", e);
			throw e;
		} finally {
		   if (cacheMode.isInvalidation())
            removeAfterEndInvalidationHandler(remoteNaturalIdCache.getAdvancedCache());

			withTxSession(localFactory, s -> {
            TEST_SESSION_ACCESS.execQueryUpdate(s, "delete NaturalIdOnManyToOne");
            TEST_SESSION_ACCESS.execQueryUpdate(s, "delete Citizen");
            TEST_SESSION_ACCESS.execQueryUpdate(s, "delete State");
			});
		}
	}

   private boolean await(CountDownLatch latch) {
      assertNotNull(latch);
      try {
         log.debugf("Await latch: %s", latch);
         boolean await = latch.await(2, TimeUnit.SECONDS);
         log.debugf("Finished waiting for latch, did latch reach zero? %b", await);
         return await;
      } catch (InterruptedException e) {
         // ignore;
         return false;
      }
   }

   public CountDownLatch getRemoteUpdateLatch(Cache remoteNaturalIdCache) {
      CountDownLatch latch;
      if (cacheMode.isInvalidation()) {
         latch = useTransactionalCache()
            ? expectAfterEndInvalidation(remoteNaturalIdCache.getAdvancedCache(), 1)
            : expectAfterEndInvalidation(remoteNaturalIdCache.getAdvancedCache(), 2);
      } else {
         latch = expectAfterUpdate(remoteNaturalIdCache.getAdvancedCache(), 2);
      }

      log.tracef("Created latch: %s", latch);
      return latch;
   }

	private void assertLoadedFromCache(MyListener localListener, String id) {
		for (String visited : localListener.visited){
			if (visited.contains(id))
				return;
		}
		fail("Citizen (" + id + ") should have present in the cache");
	}

	private void saveSomeCitizens(SessionFactory sf) throws Exception {
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

		withTxSession(sf, s -> {
			s.persist( australia );
			s.persist( france );
			s.persist( c1 );
			s.persist( c2 );
		});
	}

	private void getCitizenWithCriteria(SessionFactory sf) throws Exception {
		withTxSession(sf, s -> {
			State france = getState(s, "Ile de France");
			HibernateCriteriaBuilder cb = s.getCriteriaBuilder();
			CriteriaQuery<Citizen> criteria = cb.createQuery(Citizen.class);
			Root<Citizen> root = criteria.from(Citizen.class);
			criteria.where(cb.equal(root.get(Citizen_.state), france));

			s.createQuery(criteria)
					.getResultList();
		});
	}

	private void deleteCitizenWithCriteria(SessionFactory sf) throws Exception {
		withTxSession(sf, s -> {
			State france = getState(s, "Ile de France");

			HibernateCriteriaBuilder cb = s.getCriteriaBuilder();
			JpaCriteriaQuery<Citizen> criteria = cb.createQuery(Citizen.class);
			Root<Citizen> root = criteria.from(Citizen.class);
			criteria.where(cb.equal(root.get(Citizen_.ssn), "1234"), cb.equal(root.get(Citizen_.state), france));

			Citizen c = s.createQuery(criteria).uniqueResult();
			s.remove(c);
		});
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

	@Listener
	public static class MyListener {
		private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog( MyListener.class );
		private final Set<String> visited = ConcurrentHashMap.newKeySet();

      public MyListener(String name) {
      }

		public void clear() {
			visited.clear();
		}

		public boolean isEmpty() {
			return visited.isEmpty();
		}

		@CacheEntryVisited
		public void nodeVisited(CacheEntryVisitedEvent event) {
			log.debug( event.toString() );
			if ( !event.isPre() ) {
				visited.add(event.getKey().toString());
			}
		}

		@CacheEntryCreated
      @CacheEntryModified
      @CacheEntryRemoved
		public void nodeWritten(CacheEntryEvent event) {
		   log.debug( event.toString() );
      }
	}

}
