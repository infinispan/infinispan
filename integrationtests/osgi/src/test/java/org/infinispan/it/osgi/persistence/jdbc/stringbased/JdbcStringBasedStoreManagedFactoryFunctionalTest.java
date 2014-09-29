package org.infinispan.it.osgi.persistence.jdbc.stringbased;

import static org.infinispan.it.osgi.util.IspnKarafOptions.perSuiteOptions;
import static org.ops4j.pax.exam.CoreOptions.options;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.it.osgi.persistence.jdbc.UnitTestDatabaseManager;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerSuite;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;

/**
 * @author mgencur
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerSuite.class)
@Category(PerSuite.class)
public class JdbcStringBasedStoreManagedFactoryFunctionalTest extends JdbcStringBasedStoreFunctionalTest {
   @Configuration
   public Option[] config() throws Exception {
      return options(perSuiteOptions());
   }

   @Before
   public void setUp() {
      Bundle bundle = FrameworkUtil.getBundle(getClass());
      BundleContext bundleContext = bundle.getBundleContext();
      
      org.h2.jdbcx.JdbcDataSource service = new org.h2.jdbcx.JdbcDataSource();
      service.setURL("jdbc:h2:mem:%s;DB_CLOSE_DELAY=-1");
      service.setUser("sa");
      service.setPassword("");
      bundleContext.registerService(javax.sql.DataSource.class, service, null);
   }

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence, boolean preload) {
      JdbcStringBasedStoreConfigurationBuilder store = persistence
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class)
            .preload(preload);
      store.dataSource().jndiUrl("osgi:service/javax.sql.DataSource");
      UnitTestDatabaseManager.buildTableManipulation(store.table(), false);
      UnitTestDatabaseManager.setDialect(store);
      return persistence;
   }
}
