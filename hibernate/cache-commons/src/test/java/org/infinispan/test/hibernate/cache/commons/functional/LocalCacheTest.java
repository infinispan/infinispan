package org.infinispan.test.hibernate.cache.commons.functional;

import static org.infinispan.hibernate.cache.spi.InfinispanProperties.INFINISPAN_CONFIG_LOCAL_RESOURCE;
import static org.infinispan.hibernate.cache.spi.InfinispanProperties.INFINISPAN_CONFIG_RESOURCE_PROP;
import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.engine.transaction.jta.platform.internal.NoJtaPlatform;
import org.hibernate.resource.transaction.backend.jdbc.internal.JdbcResourceLocalTransactionCoordinatorBuilderImpl;
import org.hibernate.testing.TestForIssue;
import org.infinispan.commons.util.ByRef;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.Exceptions;
import org.infinispan.test.TestException;
import org.infinispan.test.hibernate.cache.commons.functional.entities.Customer;
import org.junit.Test;

public class LocalCacheTest extends SingleNodeTest {
   @Override
   public List<Object[]> getParameters() {
      return Collections.singletonList(new Object[]{
            "local", NoJtaPlatform.class, JdbcResourceLocalTransactionCoordinatorBuilderImpl.class, null, AccessType.READ_WRITE, CacheMode.LOCAL, false
      });
   }

   @Override
   protected void addSettings(Map settings) {
      super.addSettings(settings);
      settings.put(INFINISPAN_CONFIG_RESOURCE_PROP, INFINISPAN_CONFIG_LOCAL_RESOURCE);
   }

   @Test
   @TestForIssue(jiraKey = "HHH-12457")
   public void testRollback() throws Exception {
      ByRef<Integer> idRef = new ByRef<>(0);
      withTxSession(s -> {
         Customer c = new Customer();
         c.setName("Foo");
         s.persist(c);
         idRef.set(c.getId());
      });
      Exceptions.expectException(TestException.class, () -> withTxSession(s -> {
         Customer c = s.load(Customer.class, idRef.get());
         c.setName("Bar");
         s.persist(c);
         s.flush();
         throw new TestException("Roll me back");
      }));
      withTxSession(s -> {
         Customer c = s.load(Customer.class, idRef.get());
         assertEquals("Foo", c.getName());
      });
   }
}
