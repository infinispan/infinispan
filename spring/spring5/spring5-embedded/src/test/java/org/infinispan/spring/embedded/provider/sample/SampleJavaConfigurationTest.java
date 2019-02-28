package org.infinispan.spring.embedded.provider.sample;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.infinispan.spring.common.InfinispanTestExecutionListener;
import org.infinispan.spring.embedded.builders.SpringEmbeddedCacheManagerFactoryBeanBuilder;
import org.infinispan.spring.embedded.provider.SpringEmbeddedCacheManager;
import org.infinispan.spring.embedded.provider.sample.service.CachedBookService;
import org.infinispan.spring.embedded.provider.sample.service.CachedBookServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.testng.annotations.Test;

/**
 * Tests using Java-based cache configuration.
 *
 * @author Matej Cimbora (mcimbora@redhat.com)
 */
@Test(testName = "spring.embedded.provider.SampleJavaConfigurationTest", groups = "functional", sequential = true)
@ContextConfiguration(classes = SampleJavaConfigurationTest.ContextConfiguration.class, loader = AnnotationConfigContextLoader.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestExecutionListeners(InfinispanTestExecutionListener.class)
public class SampleJavaConfigurationTest extends AbstractTestTemplateJsr107 {

   @Autowired(required = true)
   private SpringEmbeddedCacheManager cacheManager;

   @Qualifier(value = "cachedBookService")
   @Autowired(required = true)
   private CachedBookService bookService;

   @Override
   public CachedBookService getBookService() {
      return bookService;
   }

   @Override
   public CacheManager getCacheManager() {
      return cacheManager;
   }

   @org.springframework.context.annotation.Configuration
   @ComponentScan(basePackages = {
         "org.infinispan.spring.embedded.provider.sample.dao",
         "org.infinispan.spring.embedded.provider.sample.generators",
         "org.infinispan.spring.embedded.provider.sample.resolvers",
         "org.infinispan.spring.embedded.provider.sample.service"})
   @EnableCaching
   @EnableTransactionManagement
   static class ContextConfiguration {

      @Value("classpath:/org/infinispan/spring/embedded/provider/sample/initDB.sql")
      private Resource initScript;

      @Value("classpath:/org/infinispan/spring/embedded/provider/sample/populateDB.sql")
      private Resource populateScript;


      @Bean
      public CachedBookService cachedBookService() {
         return new CachedBookServiceImpl();
      }

      @Bean(destroyMethod = "stop")
      public SpringEmbeddedCacheManager cacheManager() throws Exception {
         return SpringEmbeddedCacheManagerFactoryBeanBuilder
               .defaultBuilder()
               .fromFile("books-infinispan-config.xml", getClass())
               .build()
               .getObject();
      }

      @Bean(destroyMethod = "close")
      public DataSource basicDataSource() {
         BasicDataSource dataSource = new BasicDataSource();
         dataSource.setDriverClassName("org.h2.Driver");
         dataSource.setUrl("jdbc:h2:mem:bookstoreEnableCaching");
         dataSource.setUsername("sa");
         dataSource.setPassword("");
         return dataSource;
      }

      @Bean
      public DataSourceInitializer dataSourceInitializer(final DataSource dataSource) {
         DataSourceInitializer initializer = new DataSourceInitializer();
         initializer.setDataSource(dataSource);
         ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
         populator.addScript(initScript);
         populator.addScript(populateScript);
         initializer.setDatabasePopulator(populator);
         return initializer;
      }

      @Bean
      public DataSourceTransactionManager transactionManager(final DataSource dataSource) {
         return new DataSourceTransactionManager(dataSource);
      }
   }
}
