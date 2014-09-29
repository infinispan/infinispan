package org.infinispan.it.osgi.persistence.jdbc.mixed;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.it.osgi.persistence.jdbc.UnitTestDatabaseManager;
import org.infinispan.it.osgi.persistence.jdbc.stringbased.JdbcStringBasedStoreFunctionalTest;
import org.infinispan.persistence.jdbc.configuration.JdbcMixedStoreConfigurationBuilder;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerSuite;

/**
 * @author mgencur
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerSuite.class)
@Category(PerSuite.class)
public class JdbcMixedStoreFunctionalTest extends JdbcStringBasedStoreFunctionalTest {

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence, boolean preload) {
      JdbcMixedStoreConfigurationBuilder store = persistence
            .addStore(JdbcMixedStoreConfigurationBuilder.class)
            .preload(preload);
      UnitTestDatabaseManager.buildTableManipulation(store.binaryTable(), true);
      UnitTestDatabaseManager.buildTableManipulation(store.stringTable(), false);
      UnitTestDatabaseManager.setDialect(store);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(store);
      return persistence;
   }
}
