package org.infinispan.test.hibernate.cache.commons.functional;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;

import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.cfg.Environment;
import org.hibernate.engine.config.spi.ConfigurationService;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.stat.Statistics;
import org.infinispan.Cache;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;
import org.infinispan.hibernate.cache.spi.InfinispanProperties;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.hibernate.cache.commons.functional.entities.Item;
import org.infinispan.test.hibernate.cache.commons.naming.TrivialInitialContext;
import org.infinispan.test.hibernate.cache.commons.naming.TrivialInitialContextFactory;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactory;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactoryProvider;
import org.junit.Test;

/**
 * @author Galder Zamarreño
 */
public class JndiRegionFactoryTest extends SingleNodeTest {
   private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog(JndiRegionFactoryTest.class);
   private static final String JNDI_NAME = "java:CacheManager";
   private Properties props;
   private boolean bindToJndi = true;
   private EmbeddedCacheManager manager;

   @Override
   public List<Object[]> getParameters() {
      return Collections.singletonList(READ_WRITE_INVALIDATION);
   }

   @Override
   protected void cleanupTest() throws Exception {
      Context ctx = new InitialContext(props);
      ctx.unbind(JNDI_NAME);
      manager.stop(); // Need to stop cos JNDI region factory does not stop it.
   }

   @Override
   protected void afterStandardServiceRegistryBuilt(StandardServiceRegistry ssr) {
      if (bindToJndi) {
         try {
            // Create an in-memory jndi
            props = new Properties();
            props.put("java.naming.factory.initial", TrivialInitialContextFactory.class.getName());
            props.put("java.naming.factory.url.pkgs", TrivialInitialContext.class.getPackage().getName());

            final String cfgFileName = (String) ssr.getService(ConfigurationService.class).getSettings().get(
                  InfinispanProperties.INFINISPAN_CONFIG_RESOURCE_PROP
            );
            manager = new DefaultCacheManager(
                  cfgFileName == null ? InfinispanProperties.DEF_INFINISPAN_CONFIG_RESOURCE : cfgFileName,
                  false
            );
            Context ctx = new InitialContext(props);
            ctx.bind(JNDI_NAME, manager);
         } catch (Exception e) {
            throw new RuntimeException("Failure to set up JNDI", e);
         }
      }
   }

   @Override
   @SuppressWarnings("unchecked")
   protected void addSettings(Map settings) {
      super.addSettings(settings);

      settings.put(InfinispanProperties.CACHE_MANAGER_RESOURCE_PROP, JNDI_NAME);
      settings.put(Environment.JNDI_CLASS, TrivialInitialContextFactory.class.getName());
      settings.put("java.naming.factory.url.pkgs", TrivialInitialContext.class.getPackage().getName());
   }

   @Test
   public void testRedeployment() throws Exception {
      addEntityCheckCache(sessionFactory());
      bindToJndi = false;
      rebuildSessionFactory();

      addEntityCheckCache(sessionFactory());
      TestRegionFactory regionFactory = TestRegionFactoryProvider.load().wrap(sessionFactory().getCache().getRegionFactory());
      Cache cache = regionFactory.getCacheManager().getCache(Item.class.getName());
      assertEquals(ComponentStatus.RUNNING, cache.getStatus());
   }

   private void addEntityCheckCache(SessionFactoryImplementor sessionFactory) throws Exception {
      Item item = new Item("chris", "Chris's Item");
      withTxSession(s -> s.persist(item));

      withTxSession(s -> {
         Item found = s.getReference(Item.class, item.getId());
         Statistics stats = sessionFactory.getStatistics();
         log.info(stats.toString());
         assertEquals(item.getDescription(), found.getDescription());
         assertEquals(0, stats.getSecondLevelCacheMissCount());
         assertEquals(1, stats.getSecondLevelCacheHitCount());
         s.remove(found);
      });
   }
}
