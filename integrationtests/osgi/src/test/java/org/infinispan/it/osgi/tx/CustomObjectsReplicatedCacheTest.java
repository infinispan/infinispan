package org.infinispan.it.osgi.tx;

import static org.infinispan.it.osgi.util.IspnKarafOptions.allOptions;

import java.io.Serializable;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
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
      return allOptions();
   }
   
   @ProbeBuilder
   public TestProbeBuilder exportTestPackages(TestProbeBuilder probeBuilder) {
       StringBuilder builder = new StringBuilder();

       /* Export all test subpackages. */
       Package[] pkgs = Package.getPackages();
       for (Package pkg : pkgs) {
           String pkgName = pkg.getName();
           if (pkgName.startsWith("org.infinispan.it.osgi")) {
               if (builder.length() > 0) {
                   builder.append(",");
               }
               builder.append(pkgName);
           }
       }

       probeBuilder.setHeader("Export-Package", builder.toString());
       return probeBuilder;
   }

   @Before
   public void setUp() {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      c.jmxStatistics().enable();
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
      System.out.println("Person" + cache(1).get("k1"));
   }
   
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
