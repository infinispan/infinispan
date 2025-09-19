package org.infinispan.test.hibernate.cache.commons.tm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Environment;
import org.hibernate.mapping.Collection;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.RootClass;
import org.hibernate.resource.transaction.backend.jta.internal.JtaTransactionCoordinatorBuilderImpl;
import org.hibernate.resource.transaction.spi.TransactionStatus;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.stat.Statistics;
import org.hibernate.testing.ServiceRegistryBuilder;
import org.hibernate.testing.jta.JtaAwareConnectionProviderImpl;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;
import org.infinispan.test.hibernate.cache.commons.functional.entities.Item;
import org.infinispan.test.hibernate.cache.commons.naming.TrivialInitialContext;
import org.infinispan.test.hibernate.cache.commons.naming.TrivialInitialContextFactory;
import org.infinispan.test.hibernate.cache.commons.util.InfinispanTestingSetup;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactoryProvider;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import jakarta.transaction.Status;
import jakarta.transaction.UserTransaction;

/**
 * This is an example test based on http://community.jboss.org/docs/DOC-14617 that shows how to interact with
 * Hibernate configured with Infinispan second level cache provider using JTA transactions.
 * <p>
 * In this test, an XADataSource wrapper is in use where we have associated our transaction manager to it so that
 * commits/rollbacks are propagated to the database as well.
 *
 * @author Galder Zamarreño
 * @since 3.5
 */
public class JBossStandaloneJtaExampleTest {
   private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog(JBossStandaloneJtaExampleTest.class);
   private static final JBossStandaloneJTAManagerLookup lookup = new JBossStandaloneJTAManagerLookup();
   Context ctx;
   private ServiceRegistry serviceRegistry;

   @Rule
   public final InfinispanTestingSetup infinispanTestIdentifier = new InfinispanTestingSetup();

   @Before
   public void setUp() throws Exception {
      ctx = createJndiContext();
      // Inject configuration to initialise transaction manager from config classloader
      lookup.init(new GlobalConfigurationBuilder().build());
      bindTransactionManager();
      bindUserTransaction();
   }

   @After
   public void tearDown() throws Exception {
      try {
         ctx.unbind("UserTransaction");
         ctx.unbind("java:/TransactionManager");
         ctx.close();
      } finally {
         if (serviceRegistry != null) {
            ServiceRegistryBuilder.destroy(serviceRegistry);
         }
      }
   }

   @Test
   public void testPersistAndLoadUnderJta() throws Exception {
      Item item;
      try (SessionFactory sessionFactory = buildSessionFactory()) {
         UserTransaction ut = (UserTransaction) ctx.lookup("UserTransaction");
         ut.begin();
         try {
            Session session = sessionFactory.openSession();
            assertEquals(TransactionStatus.ACTIVE, session.getTransaction().getStatus());
            item = new Item("anItem", "An item owned by someone");
            session.persist(item);
            // IMO the flush should not be necessary, but session.close() does not flush
            // and the item is not persisted.
            session.flush();
            session.close();
         } catch (Exception e) {
            ut.setRollbackOnly();
            throw e;
         } finally {
            if (ut.getStatus() == Status.STATUS_ACTIVE)
               ut.commit();
            else
               ut.rollback();
         }

         ut = (UserTransaction) ctx.lookup("UserTransaction");
         ut.begin();
         try {
            Session session = sessionFactory.openSession();
            assertEquals(TransactionStatus.ACTIVE, session.getTransaction().getStatus());
            Item found = session.getReference(Item.class, item.getId());
            Statistics stats = session.getSessionFactory().getStatistics();
            log.info(stats.toString());
            assertEquals(item.getDescription(), found.getDescription());
            assertEquals(0, stats.getSecondLevelCacheMissCount());
            assertEquals(1, stats.getSecondLevelCacheHitCount());
            session.remove(found);
            // IMO the flush should not be necessary, but session.close() does not flush
            // and the item is not deleted.
            session.flush();
            session.close();
         } catch (Exception e) {
            ut.setRollbackOnly();
            throw e;
         } finally {
            if (ut.getStatus() == Status.STATUS_ACTIVE)
               ut.commit();
            else
               ut.rollback();
         }

         ut = (UserTransaction) ctx.lookup("UserTransaction");
         ut.begin();
         try {
            Session session = sessionFactory.openSession();
            assertEquals(TransactionStatus.ACTIVE, session.getTransaction().getStatus());
            assertNull(session.get(Item.class, item.getId()));
            session.close();
         } catch (Exception e) {
            ut.setRollbackOnly();
            throw e;
         } finally {
            if (ut.getStatus() == Status.STATUS_ACTIVE)
               ut.commit();
            else
               ut.rollback();
         }
      }

   }

   private Context createJndiContext() throws Exception {
      Properties props = new Properties();
      props.put("java.naming.factory.initial", TrivialInitialContextFactory.class.getName());
      props.put("java.naming.factory.url.pkgs", TrivialInitialContext.class.getPackage().getName());
      return new InitialContext(props);
   }

   private void bindTransactionManager() throws Exception {
      // as JBossTransactionManagerLookup extends JNDITransactionManagerLookup we must also register the TransactionManager
      ctx.bind("java:/TransactionManager", lookup.getTransactionManager());
   }

   private void bindUserTransaction() throws Exception {
      // also the UserTransaction must be registered on jndi: org.hibernate.engine.transaction.internal.jta.JtaTransactionFactory#getUserTransaction() requires this
      ctx.bind("UserTransaction", lookup.getUserTransaction());
   }

   private SessionFactory buildSessionFactory() {
      // Extra options located in src/test/resources/hibernate.properties
      StandardServiceRegistryBuilder ssrb = new StandardServiceRegistryBuilder()
            .applySetting(Environment.DIALECT, "HSQL")
            .applySetting(Environment.HBM2DDL_AUTO, "create-drop")
            .applySetting(Environment.CONNECTION_PROVIDER, JtaAwareConnectionProviderImpl.class.getName())
            .applySetting(Environment.JNDI_CLASS, TrivialInitialContextFactory.class.getName())
            .applySetting(Environment.TRANSACTION_COORDINATOR_STRATEGY, JtaTransactionCoordinatorBuilderImpl.class.getName())
            .applySetting(Environment.CURRENT_SESSION_CONTEXT_CLASS, "jta")
            .applySetting(Environment.USE_SECOND_LEVEL_CACHE, "true")
            .applySetting(Environment.USE_QUERY_CACHE, "true")
            .applySetting(Environment.JTA_PLATFORM, new NarayanaStandaloneJtaPlatform())
            .applySetting(Environment.CACHE_REGION_FACTORY, TestRegionFactoryProvider.load().getRegionFactoryClass().getName());

      StandardServiceRegistry serviceRegistry = ssrb.build();

      MetadataSources metadataSources = new MetadataSources(serviceRegistry);
      metadataSources.addResource("org/infinispan/test/hibernate/cache/commons/functional/entities/Item.hbm.xml");

      Metadata metadata = metadataSources.buildMetadata();
      for (PersistentClass entityBinding : metadata.getEntityBindings()) {
         if (entityBinding instanceof RootClass) {
            RootClass rootClass = (RootClass) entityBinding;
            rootClass.setCacheConcurrencyStrategy("transactional");
            rootClass.setCached(true);
         }
      }
      for (Collection collectionBinding : metadata.getCollectionBindings()) {
         collectionBinding.setCacheConcurrencyStrategy("transactional");
      }

      return metadata.buildSessionFactory();
   }
}
