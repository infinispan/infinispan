/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.test.cache.infinispan.functional;
<<<<<<< HEAD
=======

<<<<<<< HEAD
<<<<<<< HEAD
import java.util.Map;
>>>>>>> HHH-9490 - Migrate from dom4j to jaxb for XML processing;
=======
>>>>>>> HHH-9747 - Import initial reworking of transaction handling (based on JdbcSession work)
=======
import java.util.Map;
>>>>>>> HHH-9803 - Checkstyle fix ups - headers
import javax.transaction.Status;
import javax.transaction.TransactionManager;

import org.hibernate.cache.infinispan.InfinispanRegionFactory;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Environment;
<<<<<<< HEAD
import org.hibernate.engine.transaction.spi.TransactionFactory;
=======
>>>>>>> HHH-9490 - Migrate from dom4j to jaxb for XML processing;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.engine.transaction.jta.platform.spi.JtaPlatform;
import org.hibernate.resource.transaction.TransactionCoordinatorBuilder;
import org.hibernate.resource.transaction.backend.jta.internal.JtaTransactionCoordinatorBuilderImpl;

<<<<<<< HEAD
<<<<<<< HEAD
import org.hibernate.testing.junit4.BaseNonConfigCoreFunctionalTestCase;
import org.hibernate.test.cache.infinispan.tm.JtaPlatformImpl;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
import org.hibernate.testing.junit.functional.FunctionalTestCase;
<<<<<<< HEAD
import org.hibernate.transaction.CMTTransactionFactory;
import org.hibernate.transaction.TransactionFactory;
=======
import org.hibernate.engine.transaction.internal.jta.CMTTransactionFactory;
<<<<<<< HEAD
>>>>>>> HHH-5949 - Migrate, complete and integrate TransactionFactory as a service
import org.hibernate.transaction.TransactionManagerLookup;
=======
>>>>>>> HHH-5949 - Migrate, complete and integrate TransactionFactory as a service
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
=======
>>>>>>> HHH-5942 - Migrate to JUnit 4
=======
import org.hibernate.testing.junit4.BaseCoreFunctionalTestCase;
>>>>>>> HHH-7197 reimport imports
=======
import org.junit.Before;

import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
>>>>>>> HHH-9490 - Migrate from dom4j to jaxb for XML processing;
=======
import org.junit.Before;

import org.hibernate.testing.junit4.BaseNonConfigCoreFunctionalTestCase;
import org.hibernate.test.cache.infinispan.tm.JtaPlatformImpl;
>>>>>>> HHH-9747 - Import initial reworking of transaction handling (based on JdbcSession work)
=======
import org.hibernate.testing.junit4.BaseNonConfigCoreFunctionalTestCase;
import org.hibernate.test.cache.infinispan.tm.JtaPlatformImpl;
import org.junit.Before;

import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
>>>>>>> HHH-9803 - Checkstyle fix ups - headers

/**
 * @author Galder Zamarreño
 * @since 3.5
 */
public abstract class SingleNodeTestCase extends BaseNonConfigCoreFunctionalTestCase {
	private static final Log log = LogFactory.getLog( SingleNodeTestCase.class );
	protected TransactionManager tm;

	@Before
	public void prepare() {
		tm = getTransactionManager();
	}

	protected TransactionManager getTransactionManager() {
		try {
			Class<? extends JtaPlatform> jtaPlatformClass = getJtaPlatform();
			if ( jtaPlatformClass == null ) {
				return null;
			}
			else {
				return jtaPlatformClass.newInstance().retrieveTransactionManager();
			}
		}
		catch (Exception e) {
			log.error( "Error", e );
			throw new RuntimeException( e );
		}
	}

<<<<<<< HEAD
   public String[] getMappings() {
      return new String[] { 
               "cache/infinispan/functional/Item.hbm.xml", 
               "cache/infinispan/functional/Customer.hbm.xml", 
               "cache/infinispan/functional/Contact.hbm.xml"};
   }
=======
	@Override
	public String[] getMappings() {
		return new String[] {
				"cache/infinispan/functional/Item.hbm.xml",
				"cache/infinispan/functional/Customer.hbm.xml",
				"cache/infinispan/functional/Contact.hbm.xml"
		};
	}
>>>>>>> HHH-5942 - Migrate to JUnit 4

	@Override
	public String getCacheConcurrencyStrategy() {
		return "transactional";
	}

	protected Class<? extends RegionFactory> getCacheRegionFactory() {
		return TestInfinispanRegionFactory.class;
	}

	protected Class<? extends TransactionCoordinatorBuilder> getTransactionCoordinatorBuilder() {
		return JtaTransactionCoordinatorBuilderImpl.class;
	}

	protected Class<? extends ConnectionProvider> getConnectionProviderClass() {
		return org.hibernate.test.cache.infinispan.tm.XaConnectionProvider.class;
	}

	protected Class<? extends JtaPlatform> getJtaPlatform() {
		return JtaPlatformImpl.class;
	}

<<<<<<< HEAD
   protected boolean getUseQueryCache() {
      return true;
   }

   public void configure(Configuration cfg) {
      super.configure(cfg);
      cfg.setProperty(Environment.USE_SECOND_LEVEL_CACHE, "true");
      cfg.setProperty(Environment.GENERATE_STATISTICS, "true");
      cfg.setProperty(Environment.USE_QUERY_CACHE, String.valueOf(getUseQueryCache()));
      cfg.setProperty(Environment.CACHE_REGION_FACTORY, getCacheRegionFactory().getName());

	   if ( getJtaPlatform() != null ) {
		   cfg.getProperties().put( JtaPlatformInitiator.JTA_PLATFORM, getJtaPlatform() );
	   }
	   cfg.setProperty( Environment.TRANSACTION_STRATEGY, getTransactionFactoryClass().getName() );
	   cfg.setProperty( Environment.CONNECTION_PROVIDER, getConnectionProviderClass().getName() );
   }

   protected void beginTx() throws Exception {
      tm.begin();
   }

   protected void setRollbackOnlyTx() throws Exception {
      tm.setRollbackOnly();
   }

   protected void setRollbackOnlyTx(Exception e) throws Exception {
      log.error("Error", e);
      tm.setRollbackOnly();
      throw e;
   }

   protected void setRollbackOnlyTxExpected(Exception e) throws Exception {
      log.debug("Expected behaivour", e);
      tm.setRollbackOnly();
   }

   protected void commitOrRollbackTx() throws Exception {
      if (tm.getStatus() == Status.STATUS_ACTIVE) tm.commit();
      else tm.rollback();
   }
   
=======
	protected boolean getUseQueryCache() {
		return true;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void addSettings(Map settings) {
		super.addSettings( settings );

		settings.put( Environment.USE_SECOND_LEVEL_CACHE, "true" );
		settings.put( Environment.GENERATE_STATISTICS, "true" );
		settings.put( Environment.USE_QUERY_CACHE, String.valueOf( getUseQueryCache() ) );
		settings.put( Environment.CACHE_REGION_FACTORY, getCacheRegionFactory().getName() );

		if ( getJtaPlatform() != null ) {
			settings.put( AvailableSettings.JTA_PLATFORM, getJtaPlatform() );
		}
		settings.put( Environment.TRANSACTION_COORDINATOR_STRATEGY, getTransactionCoordinatorBuilder().getName() );
		settings.put( Environment.CONNECTION_PROVIDER, getConnectionProviderClass().getName() );
	}

	protected void beginTx() throws Exception {
		tm.begin();
	}

	protected void setRollbackOnlyTx() throws Exception {
		tm.setRollbackOnly();
	}

	protected void setRollbackOnlyTx(Exception e) throws Exception {
		log.error( "Error", e );
		tm.setRollbackOnly();
		throw e;
	}

	protected void setRollbackOnlyTxExpected(Exception e) throws Exception {
		log.debug( "Expected behaivour", e );
		tm.setRollbackOnly();
	}

	protected void commitOrRollbackTx() throws Exception {
		if ( tm.getStatus() == Status.STATUS_ACTIVE ) {
			tm.commit();
		}
		else {
			tm.rollback();
		}
	}

<<<<<<< HEAD
>>>>>>> HHH-5942 - Migrate to JUnit 4
=======
   public static class TestInfinispanRegionFactory extends InfinispanRegionFactory {

      public TestInfinispanRegionFactory() {
         super(); // For reflection-based instantiation
      }

      @Override
      protected EmbeddedCacheManager createCacheManager(ConfigurationBuilderHolder holder) {
         return TestCacheManagerFactory.createClusteredCacheManager(holder);
      }

   }

>>>>>>> HHH-7553 Upgrade to Infinispan 5.2.0.Beta2 and fix testsuite
}