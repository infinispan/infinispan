package org.infinispan.it.osgi.persistence.jdbc.stringbased;

import static org.infinispan.it.osgi.util.IspnKarafOptions.allOptions;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.it.osgi.persistence.jdbc.UnitTestDatabaseManager;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerSuite;

/**
 * @author mgencur
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerSuite.class)
public class JdbcStringBasedStoreManagedFactoryFunctionalTest extends JdbcStringBasedStoreFunctionalTest {

   @Configuration
   public Option[] config() throws Exception {
      return allOptions();
   }

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence, boolean preload) {
      JdbcStringBasedStoreConfigurationBuilder store = persistence
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class)
            .preload(preload);
      store.dataSource().jndiUrl("osgi:service/h2ds");
      UnitTestDatabaseManager.buildTableManipulation(store.table(), false);
      return persistence;
   }
}
