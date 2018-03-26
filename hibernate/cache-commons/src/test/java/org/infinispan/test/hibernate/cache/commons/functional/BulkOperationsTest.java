/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.test.hibernate.cache.commons.functional;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hibernate.stat.SecondLevelCacheStatistics;

import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;
import org.infinispan.test.hibernate.cache.commons.util.InfinispanTestingSetup;
import org.infinispan.test.hibernate.cache.commons.functional.entities.Contact;
import org.infinispan.test.hibernate.cache.commons.functional.entities.Customer;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactory;
import org.infinispan.util.ControlledTimeService;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * BulkOperationsTestCase.
 *
 * @author Galder Zamarreño
 * @since 3.5
 */
public class BulkOperationsTest extends SingleNodeTest {

   private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog(BulkOperationsTest.class);
   private static final ControlledTimeService TIME_SERVICE = new AlwaysMoveForwardTimeService();

   @Rule
   public TestName name = new TestName();

	@Override
	public List<Object[]> getParameters() {
		return getParameters(true, true, false, true);
	}

	@ClassRule
	public static final InfinispanTestingSetup infinispanTestIdentifier = new InfinispanTestingSetup();

   @Override
   protected String getBaseForMappings() {
      return "org/infinispan/test/";
   }

   @Override
	public String[] getMappings() {
		return new String[] {
				"hibernate/cache/commons/functional/entities/Contact.hbm.xml",
				"hibernate/cache/commons/functional/entities/Customer.hbm.xml"
		};
	}

   @Override
   protected void addSettings(Map settings) {
      super.addSettings(settings);
      settings.put(TestRegionFactory.TIME_SERVICE, TIME_SERVICE);
   }

	@Test
	public void testBulkOperations() throws Throwable {
      log.infof("*** %s", name.getMethodName());

		boolean cleanedUp = false;
		try {
			createContacts();

			List<Integer> rhContacts = getContactsByCustomer( "Red Hat" );
			assertNotNull( "Red Hat contacts exist", rhContacts );
			assertEquals( "Created expected number of Red Hat contacts", 10, rhContacts.size() );

			SecondLevelCacheStatistics contactSlcs = sessionFactory()
					.getStatistics()
					.getSecondLevelCacheStatistics( Contact.class.getName() );
			assertEquals( 20, contactSlcs.getElementCountInMemory() );

			assertEquals( "Deleted all Red Hat contacts", 10, deleteContacts() );
			assertEquals( 0, contactSlcs.getElementCountInMemory() );

			List<Integer> jbContacts = getContactsByCustomer( "JBoss" );
			assertNotNull( "JBoss contacts exist", jbContacts );
			assertEquals( "JBoss contacts remain", 10, jbContacts.size() );

			for ( Integer id : rhContacts ) {
				assertNull( "Red Hat contact " + id + " cannot be retrieved", getContact( id ) );
			}
			rhContacts = getContactsByCustomer( "Red Hat" );
			if ( rhContacts != null ) {
				assertEquals( "No Red Hat contacts remain", 0, rhContacts.size() );
			}

			updateContacts( "Kabir", "Updated" );
			assertEquals( 0, contactSlcs.getElementCountInMemory() );

         // Advance so that invalidation can be seen as past and data can be loaded
         TIME_SERVICE.advance(1);

			for ( Integer id : jbContacts ) {
				Contact contact = getContact( id );
				assertNotNull( "JBoss contact " + id + " exists", contact );
				String expected = ("Kabir".equals( contact.getName() )) ? "Updated" : "2222";
				assertEquals( "JBoss contact " + id + " has correct TLF", expected, contact.getTlf() );
			}

			List<Integer> updated = getContactsByTLF( "Updated" );
			assertNotNull( "Got updated contacts", updated );
			assertEquals("Updated contacts", 5, updated.size());

			assertEquals( 10, contactSlcs.getElementCountInMemory() );
			updateContactsWithOneManual( "Kabir", "UpdatedAgain" );
			assertEquals( 0, contactSlcs.getElementCountInMemory());
			for ( Integer id : jbContacts ) {
				Contact contact = getContact( id );
				assertNotNull( "JBoss contact " + id + " exists", contact );
				String expected = ("Kabir".equals( contact.getName() )) ? "UpdatedAgain" : "2222";
				assertEquals( "JBoss contact " + id + " has correct TLF", expected, contact.getTlf() );
			}

			updated = getContactsByTLF( "UpdatedAgain" );
			assertNotNull( "Got updated contacts", updated );
			assertEquals( "Updated contacts", 5, updated.size() );
		}
		catch (Throwable t) {
			cleanedUp = true;
			cleanup( true );
			throw t;
		}
		finally {
			// cleanup the db so we can run this test multiple times w/o restarting the cluster
			if ( !cleanedUp ) {
				cleanup( false );
			}
		}
	}

	public void createContacts() throws Exception {
		withTxSession(s -> {
			for ( int i = 0; i < 10; i++ ) {
				Customer c = createCustomer( i );
				s.persist(c);
			}
		});
	}

	public int deleteContacts() throws Exception {
		String deleteHQL = "delete Contact where customer in "
			+ " (select customer FROM Customer as customer where customer.name = :cName)";

		int rowsAffected = withTxSessionApply(s ->
				TEST_SESSION_ACCESS.execQueryUpdateAutoFlush(s, deleteHQL, new String[]{"cName", "Red Hat"}));
		return rowsAffected;
	}

	@SuppressWarnings( {"unchecked"})
	public List<Integer> getContactsByCustomer(String customerName) throws Exception {
		String selectHQL = "select contact.id from Contact contact"
			+ " where contact.customer.name = :cName";

		return (List<Integer>) withTxSessionApply(s ->
         TEST_SESSION_ACCESS.execQueryListAutoFlush(s, selectHQL, new String[]{"cName", customerName}));
	}

	@SuppressWarnings( {"unchecked"})
	public List<Integer> getContactsByTLF(String tlf) throws Exception {
		String selectHQL = "select contact.id from Contact contact"
			+ " where contact.tlf = :cTLF";

		return (List<Integer>) withTxSessionApply(s ->
         TEST_SESSION_ACCESS.execQueryListAutoFlush(s, selectHQL, new String[]{"cTLF", tlf}));
	}

	public int updateContacts(String name, String newTLF) throws Exception {
		String updateHQL = "update Contact set tlf = :cNewTLF where name = :cName";
		return withTxSessionApply(s ->
         TEST_SESSION_ACCESS.execQueryUpdateAutoFlush(s, updateHQL,
            new String[]{"cNewTLF", newTLF},
            new String[]{"cName", name}));
	}

	public int updateContactsWithOneManual(String name, String newTLF) throws Exception {
		String queryHQL = "from Contact c where c.name = :cName";
		String updateHQL = "update Contact set tlf = :cNewTLF where name = :cName";
		return withTxSessionApply(s -> {
			List<Contact> list = TEST_SESSION_ACCESS.execQueryList(s, queryHQL, new String[]{"cName", name});
			list.get(0).setTlf(newTLF);
			return TEST_SESSION_ACCESS.execQueryUpdateAutoFlush(s, updateHQL,
               new String[]{"cNewTLF", newTLF},
               new String[]{"cName", name});
		});
	}

	public Contact getContact(Integer id) throws Exception {
		return withTxSessionApply(s -> s.get( Contact.class, id ));
	}

	public void cleanup(boolean ignore) throws Exception {
		String deleteContactHQL = "delete from Contact";
		String deleteCustomerHQL = "delete from Customer";
		withTxSession(s -> {
         TEST_SESSION_ACCESS.execQueryUpdateAutoFlush(s, deleteContactHQL);
         TEST_SESSION_ACCESS.execQueryUpdateAutoFlush(s, deleteCustomerHQL);
		});
	}

	private Customer createCustomer(int id) throws Exception {
		log.trace("CREATE CUSTOMER " + id);
		try {
			Customer customer = new Customer();
			customer.setName( (id % 2 == 0) ? "JBoss" : "Red Hat" );
			Set<Contact> contacts = new HashSet<Contact>();

			Contact kabir = new Contact();
			kabir.setCustomer( customer );
			kabir.setName( "Kabir" );
			kabir.setTlf( "1111" );
			contacts.add( kabir );

			Contact bill = new Contact();
			bill.setCustomer( customer );
			bill.setName( "Bill" );
			bill.setTlf( "2222" );
			contacts.add( bill );

			customer.setContacts( contacts );

			return customer;
		}
		finally {
			log.trace("CREATE CUSTOMER " + id + " -  END");
		}
	}

   /**
    * A time service that whenever you ask it for the time, it moves the clock forward.
    *
    * This time service guarantees that after a session timestamp has been generated,
    * the next time the region is invalidated and the time is checked, this time is forward in time.
    *
    * By doing that, we can guarantee that when a region is invalidated in same session,
    * the region invalidation timestamp will be in the future and we avoid potential invalid things being added to second level cache.
    * More info can be found in {@link org.infinispan.hibernate.cache.commons.access.FutureUpdateSynchronization}.
    */
   private static final class AlwaysMoveForwardTimeService extends ControlledTimeService {

      @Override
      public long wallClockTime() {
         advance(1);
         return super.wallClockTime();
      }

   }

}
