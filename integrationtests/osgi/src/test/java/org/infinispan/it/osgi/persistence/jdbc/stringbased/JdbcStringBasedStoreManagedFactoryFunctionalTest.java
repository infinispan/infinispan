package org.infinispan.it.osgi.persistence.jdbc.stringbased;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.it.osgi.Osgi;
import org.infinispan.it.osgi.persistence.jdbc.UnitTestDatabaseManager;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.infinispan.it.osgi.util.IspnKarafOptions.*;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.options;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.keepRuntimeFolder;

/**
 * @author mgencur
 */
@RunWith(PaxExam.class)
@Category(Osgi.class)
@ExamReactorStrategy(PerClass.class)
public class JdbcStringBasedStoreManagedFactoryFunctionalTest extends JdbcStringBasedStoreFunctionalTest {

   @Configuration
   public Option[] config() throws Exception {
      return options(
            karafContainer(),
            featureIspnCoreDependencies(),
            featureIspnCorePlusTests(),
            featureJdbcStoreManaged(),
            junitBundles(),
            keepRuntimeFolder()
      );
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
