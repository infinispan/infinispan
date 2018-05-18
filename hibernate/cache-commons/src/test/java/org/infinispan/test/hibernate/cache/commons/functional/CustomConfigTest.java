package org.infinispan.test.hibernate.cache.commons.functional;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.persistence.Cacheable;
import javax.persistence.Entity;
import javax.persistence.Id;

import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.testing.TestForIssue;
import org.infinispan.hibernate.cache.commons.InfinispanBaseRegion;
import org.infinispan.hibernate.cache.spi.InfinispanProperties;
import org.infinispan.test.hibernate.cache.commons.util.TestSessionAccess;
import org.junit.Test;

@TestForIssue(jiraKey = "ISPN-8836")
public class CustomConfigTest extends AbstractFunctionalTest {

   public static final TestSessionAccess TEST_SESSION_ACCESS = TestSessionAccess.findTestSessionAccess();

   @Override
   public List<Object[]> getParameters() {
      return Collections.singletonList(READ_WRITE_REPLICATED);
   }

   @Override
   protected void addSettings(Map settings) {
      super.addSettings(settings);
      settings.put(AvailableSettings.CACHE_REGION_PREFIX, ""); // force no prefix
      settings.put(InfinispanProperties.INFINISPAN_CONFIG_RESOURCE_PROP, "alternative-infinispan-configs.xml");
      // myregion does not have this setting, should produce a warning
      settings.put(InfinispanProperties.PREFIX + "otherregion" + InfinispanProperties.CONFIG_SUFFIX, "otherregion");
   }

   @Override
   protected Class[] getAnnotatedClasses() {
      return new Class[] { TestEntity.class, OtherTestEntity.class };
   }

   @Test
   public void testCacheWithRegionName() {
      InfinispanBaseRegion myregion = TEST_SESSION_ACCESS.getRegion(sessionFactory(), "myregion");
      InfinispanBaseRegion otherregion = TEST_SESSION_ACCESS.getRegion(sessionFactory(), "otherregion");
      assertEquals("myregion", myregion.getCache().getName());
      assertEquals("otherregion", otherregion.getCache().getName());
   }

   @Entity
   @Cacheable
   @Cache(usage = CacheConcurrencyStrategy.READ_WRITE, region = "myregion")
   public class TestEntity {
      @Id
      long id;
      String foobar;
   }

   @Entity
   @Cacheable
   @Cache(usage = CacheConcurrencyStrategy.READ_WRITE, region = "otherregion")
   public class OtherTestEntity {
      @Id
      long id;
      String foobar;
   }
}
