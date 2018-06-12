/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.test.hibernate.cache.commons.functional.cluster;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.hibernate.cache.commons.InfinispanBaseRegion;
import org.infinispan.hibernate.cache.spi.InfinispanProperties;
import org.infinispan.hibernate.cache.commons.access.PutFromLoadValidator;
import org.infinispan.hibernate.cache.commons.util.FutureUpdate;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;
import org.hibernate.cache.spi.access.AccessType;
import org.infinispan.test.hibernate.cache.commons.functional.entities.Contact;
import org.infinispan.test.hibernate.cache.commons.functional.entities.Customer;
import org.infinispan.test.hibernate.cache.commons.util.ExpectingInterceptor;
import org.hibernate.testing.TestForIssue;
import org.infinispan.AdvancedCache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.ConcurrentHashSet;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited;
import org.infinispan.notifications.cachelistener.event.CacheEntryVisitedEvent;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactory;
import org.infinispan.test.hibernate.cache.commons.util.TestSessionAccess;
import org.infinispan.util.ControlledTimeService;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 * EntityCollectionInvalidationTestCase.
 *
 * @author Galder Zamarreño
 * @since 3.5
 */
public class EntityCollectionInvalidationTest extends DualNodeTest {
	private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog( EntityCollectionInvalidationTest.class );

	private static final Integer CUSTOMER_ID = new Integer( 1 );
	private static final TestSessionAccess TEST_SESSION_ACCESS = TestSessionAccess.findTestSessionAccess();

	private EmbeddedCacheManager localManager, remoteManager;
	private AdvancedCache localCustomerCache, remoteCustomerCache;
	private AdvancedCache localContactCache, remoteContactCache;
	private AdvancedCache localCollectionCache, remoteCollectionCache;
	private MyListener localListener, remoteListener;
	private SessionFactoryImplementor localFactory, remoteFactory;
	private final ControlledTimeService timeService = new ControlledTimeService();
	private InfinispanBaseRegion localCustomerRegion, remoteCustomerRegion;
	private InfinispanBaseRegion localCollectionRegion, remoteCollectionRegion;
	private InfinispanBaseRegion localContactRegion, remoteContactRegion;

   @Rule
   public TestName name = new TestName();


	@Override
	public List<Object[]> getParameters() {
		return getParameters(true, true, false, true);
	}

	@Override
	public void startUp()  {
		super.startUp();
		// Bind a listener to the "local" cache
		// Our region factory makes its CacheManager available to us
		localManager = ClusterAware.getCacheManager( DualNodeTest.LOCAL );
		// Cache localCache = localManager.getCache("entity");
		localCustomerCache = localManager.getCache( Customer.class.getName() ).getAdvancedCache();
		localContactCache = localManager.getCache( Contact.class.getName() ).getAdvancedCache();
		localCollectionCache = localManager.getCache( Customer.class.getName() + ".contacts" ).getAdvancedCache();
		localListener = new MyListener( "local" );
		localCustomerCache.addListener( localListener );
		localContactCache.addListener( localListener );
		localCollectionCache.addListener( localListener );

		// Bind a listener to the "remote" cache
		remoteManager = ClusterAware.getCacheManager( DualNodeTest.REMOTE );
		remoteCustomerCache = remoteManager.getCache( Customer.class.getName() ).getAdvancedCache();
		remoteContactCache = remoteManager.getCache( Contact.class.getName() ).getAdvancedCache();
		remoteCollectionCache = remoteManager.getCache( Customer.class.getName() + ".contacts" ).getAdvancedCache();
		remoteListener = new MyListener( "remote" );
		remoteCustomerCache.addListener( remoteListener );
		remoteContactCache.addListener( remoteListener );
		remoteCollectionCache.addListener( remoteListener );

		localFactory = sessionFactory();
		localCustomerRegion = TEST_SESSION_ACCESS.getRegion(localFactory, Customer.class.getName());
		localContactRegion = TEST_SESSION_ACCESS.getRegion(localFactory, Contact.class.getName());
		localCollectionRegion = TEST_SESSION_ACCESS.getRegion(localFactory, Customer.class.getName() + ".contacts");
		remoteFactory = secondNodeEnvironment().getSessionFactory();
		remoteCustomerRegion = TEST_SESSION_ACCESS.getRegion(remoteFactory, Customer.class.getName());
		remoteContactRegion = TEST_SESSION_ACCESS.getRegion(remoteFactory, Contact.class.getName());
		remoteCollectionRegion = TEST_SESSION_ACCESS.getRegion(remoteFactory, Customer.class.getName() + ".contacts");

	}

	@Override
	public void shutDown() {
		cleanupTransactionManagement();
	}

	@Override
	protected void cleanupTest() throws Exception {
		cleanup(localFactory);
		localListener.clear();
		remoteListener.clear();
		// do not call super.cleanupTest becasue we would clean transaction managers
	}

	@Override
	protected void addSettings(Map settings) {
		super.addSettings(settings);
		settings.put(TestRegionFactory.PENDING_PUTS_SIMPLE, false);
		settings.put(TestRegionFactory.TIME_SERVICE, timeService);
	}

	@Test
	public void testAll() throws Exception {
      log.infof(name.getMethodName());

		assertEmptyCaches();
		assertTrue( remoteListener.isEmpty() );
		assertTrue( localListener.isEmpty() );

		log.debug( "Create node 0" );
		IdContainer ids = createCustomer( localFactory );

		assertTrue( remoteListener.isEmpty() );
		assertTrue( localListener.isEmpty() );

		// The create customer above removes the collection from the cache (see CollectionRecreateAction)
		// and therefore the putFromLoad in getCustomer could be considered stale if executed too soon.
		timeService.advance(1);

      CountDownLatch remoteCollectionLoadLatch = null;
      if (!cacheMode.isInvalidation()) {
         remoteCollectionLoadLatch = new CountDownLatch(1);
         ExpectingInterceptor.get(remoteCollectionCache)
            .when((ctx, cmd) -> cmd instanceof ReadWriteKeyCommand)
            .countDown(remoteCollectionLoadLatch);
      }

		log.debug( "Find node 0" );
		// This actually brings the collection into the cache
		getCustomer( ids.customerId, localFactory );

		// Now the collection is in the cache so, the 2nd "get"
		// should read everything from the cache
		log.debug( "Find(2) node 0" );
		localListener.clear();
		getCustomer( ids.customerId, localFactory );

		// Check the read came from the cache
		log.debug( "Check cache 0" );
		assertLoadedFromCache( localListener, ids.customerId, ids.contactIds );

      if (remoteCollectionLoadLatch != null) {
         log.debug( "Wait for remote collection put from load to complete" );
         assertTrue(remoteCollectionLoadLatch.await(2, TimeUnit.SECONDS));
         ExpectingInterceptor.cleanup(remoteCollectionCache);
      }

		log.debug( "Find node 1" );
		// This actually brings the collection into the cache since invalidation is in use
		getCustomer( ids.customerId, remoteFactory );

		// Now the collection is in the cache so, the 2nd "get"
		// should read everything from the cache
		log.debug( "Find(2) node 1" );
		remoteListener.clear();
		getCustomer( ids.customerId, remoteFactory );

		// Check the read came from the cache
		log.debug( "Check cache 1" );
		assertLoadedFromCache( remoteListener, ids.customerId, ids.contactIds );

		// Modify customer in remote
		remoteListener.clear();

		CountDownLatch modifyLatch = null;
		if (!cacheMode.isInvalidation() && accessType != AccessType.NONSTRICT_READ_WRITE) {
			modifyLatch = new CountDownLatch(1);
			ExpectingInterceptor.get(localCustomerCache).when(this::isFutureUpdate).countDown(modifyLatch);
		}

		ids = modifyCustomer( ids.customerId, remoteFactory );
		assertLoadedFromCache( remoteListener, ids.customerId, ids.contactIds );

		if (modifyLatch != null) {
			assertTrue(modifyLatch.await(2, TimeUnit.SECONDS));
			ExpectingInterceptor.cleanup(localCustomerCache);
		}

		assertEquals( 0, localCollectionRegion.getElementCountInMemory());
		if (cacheMode.isInvalidation()) {
			// After modification, local cache should have been invalidated and hence should be empty
			assertEquals(0, localCustomerRegion.getElementCountInMemory());
		} else {
			// Replicated cache is updated, not invalidated
			assertEquals(1, localCustomerRegion.getElementCountInMemory());
		}
	}

	@TestForIssue(jiraKey = "HHH-9881")
	@Test
	public void testConcurrentLoadAndRemoval() throws Exception {
		if (!remoteCustomerCache.getCacheConfiguration().clustering().cacheMode().isInvalidation()) {
			// This test is tailored for invalidation-based strategies, using pending puts cache
			return;
		}
		AtomicReference<Exception> getException = new AtomicReference<>();
		AtomicReference<Exception> deleteException = new AtomicReference<>();

		Phaser getPhaser = new Phaser(2);
		HookInterceptor hookInterceptor = new HookInterceptor(getException);
		AdvancedCache remotePPCache = remoteCustomerCache.getCacheManager().getCache(
				remoteCustomerCache.getName() + "-" + InfinispanProperties.DEF_PENDING_PUTS_RESOURCE).getAdvancedCache();
		remotePPCache.getAdvancedCache().addInterceptor(hookInterceptor, 0);

		IdContainer idContainer = new IdContainer();
		withTxSession(localFactory, s -> {
			Customer customer = new Customer();
			customer.setName( "JBoss" );
			s.persist(customer);
			idContainer.customerId = customer.getId();
		});
		// start loading

		Thread getThread = new Thread(() -> {
			try {
				withTxSession(remoteFactory, s -> {
					s.get(Customer.class, idContainer.customerId);
				});
			} catch (Exception e) {
				log.error("Failure to get customer", e);
				getException.set(e);
			}
		}, "get-thread");
		Thread deleteThread = new Thread(() -> {
			try {
				withTxSession(localFactory, s -> {
					Customer customer = s.get(Customer.class, idContainer.customerId);
					s.delete(customer);
				});
			} catch (Exception e) {
				log.error("Failure to delete customer", e);
				deleteException.set(e);
			}
		}, "delete-thread");
		// get thread should block on the beginning of PutFromLoadValidator#acquirePutFromLoadLock
		hookInterceptor.block(getPhaser, getThread);
		getThread.start();

		arriveAndAwait(getPhaser);
		deleteThread.start();
		deleteThread.join();
		hookInterceptor.unblock();
		arriveAndAwait(getPhaser);
		getThread.join();

		if (getException.get() != null) {
			throw new IllegalStateException("get-thread failed", getException.get());
		}
		if (deleteException.get() != null) {
			throw new IllegalStateException("delete-thread failed", deleteException.get());
		}

		Customer localCustomer = getCustomer(idContainer.customerId, localFactory);
		assertNull(localCustomer);
		Customer remoteCustomer = getCustomer(idContainer.customerId, remoteFactory);
		assertNull(remoteCustomer);
		assertTrue(remoteCustomerCache.isEmpty());
	}

	protected void assertEmptyCaches() {
		assertEquals(0, localCustomerRegion.getElementCountInMemory());
		assertEquals(0, localContactRegion.getElementCountInMemory());
		assertEquals(0, localCollectionRegion.getElementCountInMemory());
		assertEquals(0, remoteCustomerRegion.getElementCountInMemory());
		assertEquals(0, remoteContactRegion.getElementCountInMemory());
		assertEquals(0, remoteCollectionRegion.getElementCountInMemory());
	}

	private IdContainer createCustomer(SessionFactory sessionFactory)
			throws Exception {
		log.debug( "CREATE CUSTOMER" );

		Customer customer = new Customer();
		customer.setName("JBoss");
		Set<Contact> contacts = new HashSet<Contact>();

		Contact kabir = new Contact();
		kabir.setCustomer(customer);
		kabir.setName("Kabir");
		kabir.setTlf("1111");
		contacts.add(kabir);

		Contact bill = new Contact();
		bill.setCustomer(customer);
		bill.setName("Bill");
		bill.setTlf("2222");
		contacts.add(bill);

		customer.setContacts(contacts);

		ArrayList<Runnable> cleanup = new ArrayList<>();
		CountDownLatch customerLatch = new CountDownLatch(1);
		CountDownLatch collectionLatch = new CountDownLatch(1);
		CountDownLatch contactsLatch = new CountDownLatch(2);

		if (cacheMode.isInvalidation()) {
			cleanup.add(mockValidator(remoteCustomerCache, customerLatch));
			cleanup.add(mockValidator(remoteCollectionCache, collectionLatch));
			cleanup.add(mockValidator(remoteContactCache, contactsLatch));
		} else if (accessType == AccessType.NONSTRICT_READ_WRITE) {
			// ATM nonstrict mode has sync after-invalidation update
			Stream.of(customerLatch, collectionLatch, contactsLatch, contactsLatch).forEach(l -> l.countDown());
		} else {
			ExpectingInterceptor.get(remoteCustomerCache).when(this::isFutureUpdate).countDown(collectionLatch);
			ExpectingInterceptor.get(remoteCollectionCache).when(this::isFutureUpdate).countDown(customerLatch);
			ExpectingInterceptor.get(remoteContactCache).when(this::isFutureUpdate).countDown(contactsLatch);
			cleanup.add(() -> ExpectingInterceptor.cleanup(remoteCustomerCache, remoteCollectionCache, remoteContactCache));
		}

		withTxSession(sessionFactory, session -> session.save(customer));

		assertTrue(customerLatch.await(2, TimeUnit.SECONDS));
		assertTrue(collectionLatch.await(2, TimeUnit.SECONDS));
		assertTrue(contactsLatch.await(2, TimeUnit.SECONDS));
		cleanup.forEach(Runnable::run);

		IdContainer ids = new IdContainer();
		ids.customerId = customer.getId();
		Set contactIds = new HashSet();
		contactIds.add( kabir.getId() );
		contactIds.add( bill.getId() );
		ids.contactIds = contactIds;

		log.debug( "CREATE CUSTOMER -  END" );
		return ids;
	}

	private boolean isFutureUpdate(InvocationContext ctx, VisitableCommand cmd) {
		return cmd instanceof ReadWriteKeyCommand && ((ReadWriteKeyCommand) cmd).getFunction() instanceof FutureUpdate;
	}

	private Runnable mockValidator(AdvancedCache cache, CountDownLatch latch) {
		PutFromLoadValidator originalValidator = PutFromLoadValidator.removeFromCache(cache);
		PutFromLoadValidator mockValidator = spy(originalValidator);
		doAnswer(invocation -> {
			try {
				return invocation.callRealMethod();
			} finally {
				latch.countDown();
			}
		}).when(mockValidator).endInvalidatingKey(any(), any());
		PutFromLoadValidator.addToCache(cache, mockValidator);
		return () -> {
			PutFromLoadValidator.removeFromCache(cache);
			PutFromLoadValidator.addToCache(cache, originalValidator);
		};
	}

	private Customer getCustomer(Integer id, SessionFactory sessionFactory) throws Exception {
		log.debug( "Find customer with id=" + id );
		return withTxSessionApply(sessionFactory, session -> doGetCustomer(id, session));
	}

	private Customer doGetCustomer(Integer id, Session session) throws Exception {
		Customer customer = session.get( Customer.class, id );
		if (customer == null) {
			return null;
		}
		// Access all the contacts
		Set<Contact> contacts = customer.getContacts();
		if (contacts != null) {
			for (Iterator it = contacts.iterator(); it.hasNext(); ) {
				((Contact) it.next()).getName();
			}
		}
		return customer;
	}

	private IdContainer modifyCustomer(Integer id, SessionFactory sessionFactory)
			throws Exception {
		log.debug( "Modify customer with id=" + id );
		return withTxSessionApply(sessionFactory, session -> {
			IdContainer ids = new IdContainer();
			Set contactIds = new HashSet();
			Customer customer = doGetCustomer( id, session );
			customer.setName( "NewJBoss" );
			ids.customerId = customer.getId();
			Set<Contact> contacts = customer.getContacts();
			for ( Contact c : contacts ) {
				contactIds.add( c.getId() );
			}
			Contact contact = contacts.iterator().next();
			contacts.remove( contact );
			contactIds.remove( contact.getId() );
			ids.contactIds = contactIds;
			contact.setCustomer( null );

			session.save( customer );
			return ids;
		});
	}

	private void cleanup(SessionFactory sessionFactory) throws Exception {
		withTxSession(sessionFactory, session -> {
			Customer c = (Customer) session.get(Customer.class, CUSTOMER_ID);
			if (c != null) {
				Set contacts = c.getContacts();
				for (Iterator it = contacts.iterator(); it.hasNext(); ) {
					session.delete(it.next());
				}
				c.setContacts(null);
				session.delete(c);
			}
			// since we don't use orphan removal, some contacts may persist
			for (Object contact : session.createCriteria(Contact.class).list()) {
				session.delete(contact);
			}
		});
	}

	private void assertLoadedFromCache(MyListener listener, Integer custId, Set contactIds) {
		assertTrue("Customer#" + custId + " was in cache", listener.visited.contains("Customer#" + custId));
		for ( Iterator it = contactIds.iterator(); it.hasNext(); ) {
			Integer contactId = (Integer) it.next();
			assertTrue("Contact#" + contactId + " was in cache", listener.visited.contains("Contact#"	+ contactId));
		}
		assertTrue("Customer.contacts" + custId + " was in cache", listener.visited.contains( "Customer.contacts#" + custId ));
	}

	protected static void arriveAndAwait(Phaser phaser) throws TimeoutException, InterruptedException {
		try {
			phaser.awaitAdvanceInterruptibly(phaser.arrive(), 10, TimeUnit.SECONDS);
		} catch (TimeoutException e) {
			log.error("Failed to progress: " + Util.threadDump());
			throw e;
		}
	}

	@Listener
	public static class MyListener {
		private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog( MyListener.class );
		private Set<String> visited = new ConcurrentHashSet<String>();
		private final String name;

		public MyListener(String name) {
			this.name = name;
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
				String key = event.getCache().getName() + "#" + event.getKey();
				log.debug( "MyListener[" + name + "] - Visiting key " + key );
				// String name = fqn.toString();
				String token = ".entities.";
				int index = key.indexOf( token );
				if ( index > -1 ) {
					index += token.length();
					key = key.substring( index );
					log.debug( "MyListener[" + this.name + "] - recording visit to " + key );
					visited.add( key );
				}
			}
		}
	}

	private class IdContainer {
		Integer customerId;
		Set<Integer> contactIds;
	}

	private static class HookInterceptor extends BaseCustomInterceptor {
		final AtomicReference<Exception> failure;
		Phaser phaser;
		Thread thread;

		private HookInterceptor(AtomicReference<Exception> failure) {
			this.failure = failure;
		}

		public synchronized void block(Phaser phaser, Thread thread) {
			this.phaser = phaser;
			this.thread = thread;
		}

		public synchronized void unblock() {
			phaser = null;
			thread = null;
		}

		@Override
		public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
			try {
				Phaser phaser;
				Thread thread;
				synchronized (this) {
					phaser = this.phaser;
					thread = this.thread;
				}
				if (phaser != null && Thread.currentThread() == thread) {
					arriveAndAwait(phaser);
					arriveAndAwait(phaser);
				}
			} catch (Exception e) {
				failure.set(e);
				throw e;
			} finally {
				return super.visitGetKeyValueCommand(ctx, command);
			}
		}
	}

}
