package org.infinispan.it.osgi.tx;

import static org.infinispan.it.osgi.util.IspnKarafOptions.perSuiteOptions;
import static org.junit.Assert.assertEquals;
import static org.ops4j.pax.exam.CoreOptions.options;

import java.io.Serializable;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.it.osgi.util.PaxExamUtils;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.ProbeBuilder;
import org.ops4j.pax.exam.TestProbeBuilder;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerSuite;

/**
 * @author mgencur
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerSuite.class)
public class CustomObjectsReplicatedCacheTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() {
      //not used
   }

   @Configuration
   public Option[] config() throws Exception {
      return options(perSuiteOptions());
   }

   @ProbeBuilder
   public TestProbeBuilder probe(TestProbeBuilder probeBuilder) {
      return PaxExamUtils.exportTestPackages(probeBuilder);
   }

   @Before
   public void setUp() {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      createCluster(c, 2);
   }

   @After
   public void tearDown() {
      TestingUtil.killCacheManagers(cacheManagers);
   }

   @Test
   public void testCustomObjectClustered() {
      Person p = new Person("Martin");
      cache(0).put("k1", p);
      assertEquals(p, cache(1).get("k1"));
   }
   
   /*
    * This custom class has to be defined here. We can't reuse a similar class from ispn-core
    * as then a CNFE error would never appear. The custom class would be visible to
    * the ispn-core OSGI module. We need the class to be provided by a client bundle.
    */
   static class Person implements Serializable {

      final String name;

      public Person(String name) {
          this.name = name;
      }

      public String getName() {
          return name;
      }

      @Override
      public boolean equals(Object o) {
          if (this == o) return true;
          if (o == null || getClass() != o.getClass()) return false;

          Person person = (Person) o;

          if (!name.equals(person.name)) return false;

          return true;
      }

      @Override
      public int hashCode() {
          return name.hashCode();
      }
  }
}
