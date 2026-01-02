package org.infinispan.query.mapper;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.Arrays;

import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.ProgrammaticMappingConfigurationContext;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.TypeMappingStep;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.mapper.mapping.MappingConfigurationContext;
import org.infinispan.query.mapper.mapping.ProgrammaticSearchMappingProvider;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.kohsuke.MetaInfServices;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.query.ProgrammaticSearchMappingProviderTest")
public class ProgrammaticSearchMappingProviderTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.indexing()
              .enable()
              .storage(LOCAL_HEAP)
              .addIndexedEntity(ProgrammaticPerson.class.getName());

      return TestCacheManagerFactory.createCacheManager(config);
   }

   public record ProgrammaticPerson(String name, String surname, int age) { }

   @MetaInfServices
   public static class MyProgrammaticSearchMappingProvider implements ProgrammaticSearchMappingProvider {

      @Override
      public void configure(MappingConfigurationContext context) {
         if (isTestRunning()) {
            ProgrammaticMappingConfigurationContext pmc = context.programmaticMapping();
            TypeMappingStep typeMapping = pmc.type(ProgrammaticPerson.class);
            typeMapping.indexed().enabled(true);
            typeMapping.property("name").fullTextField("name_analyzed");
            typeMapping.property("surname").fullTextField();
            typeMapping.property("age").genericField(); // for numeric filtering/sorting
         }
      }

      private boolean isTestRunning() {
         // Check if our specific test is in the call stack
         return Arrays.stream(Thread.currentThread().getStackTrace())
                 .anyMatch(frame -> frame.getClassName().equals(ProgrammaticSearchMappingProviderTest.class.getName()));
      }
   }

   @Test
   public void test() {
      cache.put("person1", new ProgrammaticPerson("William", "Shakespeare", 50));
      cache.query("FROM " + ProgrammaticPerson.class.getName() + " WHERE name_analyzed:'Will*'");
   }
}
