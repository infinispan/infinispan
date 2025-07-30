package org.infinispan.query.mapper;

import org.hibernate.search.mapper.pojo.mapping.definition.annotation.FullTextField;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.Indexed;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.ProgrammaticMappingConfigurationContext;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.TypeMappingStep;
import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.search.mapper.mapping.MappingConfigurationContext;
import org.infinispan.search.mapper.mapping.ProgrammaticSearchMappingProvider;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.kohsuke.MetaInfServices;
import org.testng.annotations.Test;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

@Test(groups = "functional", testName = "query.query.ProgrammaticSearchMappingProviderTest")
public class ProgrammaticSearchMappingProviderTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
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
         ProgrammaticMappingConfigurationContext pmc = context.programmaticMapping();
         TypeMappingStep typeMapping = pmc.type(ProgrammaticPerson.class);
         typeMapping.indexed();
         typeMapping.property("name").fullTextField();
         typeMapping.property("surname").fullTextField();
         typeMapping.property("age").genericField(); // for numeric filtering/sorting
      }
   }

   @Test
   public void test() {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.indexing()
              .enable()
              .storage(LOCAL_HEAP)
              .addIndexedEntity(ProgrammaticPerson.class.getName());

      Cache<Object, Object> indexed = cacheManager.administration()
              .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
              .getOrCreateCache("indexed", config.build());

      indexed.put("person1", new ProgrammaticPerson("William", "Shakespeare", 50));
      indexed.query("FROM " + ProgrammaticPerson.class.getName() + " WHERE name='William'");
   }
}
