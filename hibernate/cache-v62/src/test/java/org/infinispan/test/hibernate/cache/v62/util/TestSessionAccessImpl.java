package org.infinispan.test.hibernate.cache.v62.util;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.boot.registry.BootstrapServiceRegistry;
import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.cfg.spi.DomainDataCachingConfig;
import org.hibernate.cache.spi.CacheImplementor;
import org.hibernate.cache.spi.CacheTransactionSynchronization;
import org.hibernate.cache.spi.DirectAccessRegion;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.CachedDomainDataAccess;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.engine.jdbc.connections.spi.JdbcConnectionAccess;
import org.hibernate.engine.jdbc.dialect.spi.DialectFactory;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.transaction.internal.TransactionImpl;
import org.hibernate.engine.transaction.jta.platform.internal.NoJtaPlatform;
import org.hibernate.engine.transaction.jta.platform.spi.JtaPlatform;
import org.hibernate.internal.AbstractSharedSessionContract;
import org.hibernate.internal.SessionCreationOptions;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.jpa.spi.JpaCompliance;
import org.hibernate.metamodel.model.domain.NavigableRole;
import org.hibernate.query.Query;
import org.hibernate.resource.jdbc.spi.JdbcSessionContext;
import org.hibernate.resource.jdbc.spi.JdbcSessionOwner;
import org.hibernate.resource.transaction.backend.jdbc.internal.JdbcResourceLocalTransactionCoordinatorBuilderImpl;
import org.hibernate.resource.transaction.backend.jdbc.spi.JdbcResourceTransactionAccess;
import org.hibernate.resource.transaction.spi.TransactionCoordinator;
import org.hibernate.resource.transaction.spi.TransactionCoordinatorOwner;
import org.hibernate.service.internal.ProvidedService;
import org.hibernate.testing.boot.DialectFactoryTestingImpl;
import org.hibernate.testing.boot.ServiceRegistryTestingImpl;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.hibernate.cache.commons.InfinispanBaseRegion;
import org.infinispan.hibernate.cache.v62.impl.DomainDataRegionImpl;
import org.infinispan.hibernate.cache.v62.impl.Sync;
import org.infinispan.test.hibernate.cache.commons.util.BatchModeJtaPlatform;
import org.infinispan.test.hibernate.cache.commons.util.JdbcResourceTransactionMock;
import org.infinispan.test.hibernate.cache.commons.util.TestSessionAccess;
import org.infinispan.test.hibernate.cache.commons.util.TestingUtil;
import org.kohsuke.MetaInfServices;
import org.mockito.Mockito;

import jakarta.persistence.FlushModeType;
import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;

@MetaInfServices(TestSessionAccess.class)
public class TestSessionAccessImpl implements TestSessionAccess {

   @Override
   public Object mockSessionImplementor() {
      return mock(SharedSessionContractImplementor.class);
   }

   @Override
   public Object mockSession(Class<? extends JtaPlatform> jtaPlatform, ControlledTimeService timeService, RegionFactory regionFactory) {
      SessionMock session = mock(SessionMock.class, withSettings().extraInterfaces(Session.class));
      when(session.isClosed()).thenReturn(false);
      when(session.isOpen()).thenReturn(true);

      TransactionCoordinator txCoord;
      if (jtaPlatform == BatchModeJtaPlatform.class) {
         BatchModeTransactionCoordinator batchModeTxCoord = new BatchModeTransactionCoordinator();
         txCoord = batchModeTxCoord;
         when(session.getTransactionCoordinator()).thenReturn(txCoord);
         when(session.beginTransaction()).then(invocation -> {
            Transaction tx = batchModeTxCoord.newTransaction();
            tx.begin();
            return tx;
         });
      } else if (jtaPlatform == null || jtaPlatform == NoJtaPlatform.class) {
         Connection connection = mock(Connection.class);
         JdbcConnectionAccess jdbcConnectionAccess = mock(JdbcConnectionAccess.class);
         try {
            when(jdbcConnectionAccess.obtainConnection()).thenReturn(connection);
         } catch (SQLException e) {
            // never thrown from mock
         }
         JdbcSessionOwner jdbcSessionOwner = mock(JdbcSessionOwner.class);
         when(jdbcSessionOwner.getJdbcConnectionAccess()).thenReturn(jdbcConnectionAccess);
         SqlExceptionHelper sqlExceptionHelper = mock(SqlExceptionHelper.class);
         JdbcServices jdbcServices = mock(JdbcServices.class);
         when(jdbcServices.getSqlExceptionHelper()).thenReturn(sqlExceptionHelper);

         JdbcSessionContext jdbcSessionContext = mock(JdbcSessionContext.class);
         when(jdbcSessionContext.getJdbcServices()).thenReturn(jdbcServices);
         JpaCompliance jpaCompliance = mock(JpaCompliance.class);
         when(jpaCompliance.isJpaTransactionComplianceEnabled()).thenReturn(true);
         SessionFactoryOptions sessionFactoryOptions = mock(SessionFactoryOptions.class);
         when(sessionFactoryOptions.getJpaCompliance()).thenReturn(jpaCompliance);

         SessionFactoryImplementor sessionFactory = mock(SessionFactoryImplementor.class);
         when(sessionFactory.getSessionFactoryOptions()).thenReturn(sessionFactoryOptions);
//         SessionFactoryImplementor sessionFactory = new NoOpSessionFactoryImplementor() {
//            @Override
//            public SessionFactoryOptions getSessionFactoryOptions() {
//               return sessionFactoryOptions;
//            }
//         };
         when(jdbcSessionOwner.getJdbcSessionContext()).thenReturn(jdbcSessionContext);
         when(session.getSessionFactory()).thenReturn(sessionFactory);
         when(session.getFactory()).thenReturn(sessionFactory);
         NonJtaTransactionCoordinator txOwner = mock(NonJtaTransactionCoordinator.class);
         when(txOwner.getResourceLocalTransaction()).thenReturn(new JdbcResourceTransactionMock());
         when(txOwner.getJdbcSessionOwner()).thenReturn(jdbcSessionOwner);
         when(txOwner.isActive()).thenReturn(true);
         txCoord = new JdbcResourceLocalTransactionCoordinatorBuilderImpl()
               .buildTransactionCoordinator(txOwner, null);
         when(session.getTransactionCoordinator()).thenReturn(txCoord);
         when(session.beginTransaction()).then(invocation -> {
            Transaction tx = new TransactionImpl(txCoord, session);
            tx.begin();
            return tx;
         });
      } else {
         throw new IllegalStateException("Unknown JtaPlatform: " + jtaPlatform);
      }
      Sync sync = new Sync(regionFactory);
      TestSynchronization synchronization = new TestSynchronization(sync);
      CacheTransactionSynchronization cts = mock(Sync.class, Mockito.withSettings().defaultAnswer(invocation -> {
         if (!synchronization.registered) {
            txCoord.getLocalSynchronizations().registerSynchronization(synchronization);
            synchronization.registered = true;
         }
         return invocation.getMethod().invoke(sync, invocation.getArguments());
      }));
      when(session.getCacheTransactionSynchronization()).thenReturn(cts);

      // Need to use to prevent invoking method
      doAnswer(___ -> timeService.wallClockTime()).when(cts).getCachingTimestamp();

      return session;
   }

   private static ProvidedService<DialectFactory> dialectFactoryService() {
      return new ProvidedService<>( DialectFactory.class, new DialectFactoryTestingImpl() );
   }

   private static ProvidedService<ConnectionProvider> connectionProviderService() {
      return new ProvidedService<>(
            ConnectionProvider.class,
            TestingUtil.buildConnectionProvider(true)
      );
   }

   private static ServiceRegistryTestingImpl createServiceRegistry(boolean autoCloseRegistry,
                                                                   BootstrapServiceRegistry bootstrapServiceRegistry,
                                                                   List<StandardServiceInitiator<?>> serviceInitiators,
                                                                   List<ProvidedService<?>> providedServices,
                                                                   Map<String,Object> configurationValues) {
      // The following code is so that both Hibernate 6.2 and 6.3 can both be tested. The two versions changed the
      // ServiceRegistryTestingImpl creation methods. In 6.2 we have to use its constructor and in 6.3 we can use
      // the create static method instead
      try {
         try {
            // This method exists in 6.3
            Method method = ServiceRegistryTestingImpl.class.getMethod("create", Boolean.TYPE,
                  BootstrapServiceRegistry.class, List.class, List.class, Map.class);
            return (ServiceRegistryTestingImpl) method.invoke(null, autoCloseRegistry, bootstrapServiceRegistry,
                  serviceInitiators, providedServices, configurationValues);
         } catch (NoSuchMethodException e) {
            // Falling back to 6.2 code
         }
         Constructor<ServiceRegistryTestingImpl> constructor = ServiceRegistryTestingImpl.class.getConstructor(
               Boolean.TYPE, BootstrapServiceRegistry.class, List.class, List.class, Map.class);
         return constructor.newInstance(autoCloseRegistry, bootstrapServiceRegistry, serviceInitiators,
               providedServices, configurationValues);
      } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public Transaction beginTransaction(Object session) {
      return ((Session) session).beginTransaction();
   }

   @Override
   public TestRegionAccessStrategy fromAccess(Object access) {
      return new TestRegionAccessStrategyImpl((CachedDomainDataAccess) access);
   }

   @Override
   public TestRegion fromRegion(InfinispanBaseRegion region) {
      return new TestRegionImpl((DirectAccessRegion) region);
   }

   @Override
   public List execQueryList(Object session, String query, String[]... params) {
      Query q = ((Session) session).createQuery(query);
      setParams(q, params);
      return q.list();
   }

   @Override
   public List execQueryListAutoFlush(Object session, String query, String[]... params) {
      Query q = ((Session) session).createQuery(query).setFlushMode(FlushModeType.AUTO);
      setParams(q, params);
      return q.list();
   }

   @Override
   public List execQueryListCacheable(Object session, String query) {
      return ((Session) session).createQuery(query).setCacheable(true).list();
   }

   @Override
   public int execQueryUpdateAutoFlush(Object session, String query, String[]... params) {
      Query q = ((Session) session).createQuery(query).setFlushMode(FlushModeType.AUTO);
      setParams(q, params);
      return q.executeUpdate();
   }

   public void setParams(Query q, String[][] params) {
      if (params.length > 0) {
         for (String[] param : params) {
            q.setParameter(param[0], param[1]);
         }
      }
   }

   @Override
   public void execQueryUpdate(Object session, String query) {
      ((Session) session).createQuery(query).executeUpdate();
   }

   @Override
   public Object collectionAccess(InfinispanBaseRegion region, AccessType accessType) {
      DomainDataRegionImpl impl = (DomainDataRegionImpl) region;
      NavigableRole role = impl.config().getCollectionCaching().stream()
            .filter(c -> c.getAccessType() == accessType)
            .map(DomainDataCachingConfig::getNavigableRole)
            .findFirst().orElseThrow(() -> new IllegalArgumentException());
      return impl.getCollectionDataAccess(role);
   }

   @Override
   public Object entityAccess(InfinispanBaseRegion region, AccessType accessType) {
      DomainDataRegionImpl impl = (DomainDataRegionImpl) region;
      NavigableRole role = impl.config().getEntityCaching().stream()
            .filter(c -> c.getAccessType() == accessType)
            .map(DomainDataCachingConfig::getNavigableRole)
            .findFirst().orElseThrow(() -> new IllegalArgumentException());
      return impl.getEntityDataAccess(role);
   }

   @Override
   public InfinispanBaseRegion getRegion(SessionFactoryImplementor sessionFactory, String regionName) {
      return (InfinispanBaseRegion) sessionFactory.getCache().getRegion(regionName);
   }

   @Override
   public Collection<InfinispanBaseRegion> getAllRegions(SessionFactoryImplementor sessionFactory) {
      CacheImplementor cache = sessionFactory.getCache();
      return cache.getCacheRegionNames().stream()
            .map(regionName -> (InfinispanBaseRegion) cache.getRegion(regionName))
            .collect(Collectors.toList());
   }

   private static SharedSessionContractImplementor unwrap(Object session) {
      return (SharedSessionContractImplementor) session;
   }

   private static final class TestRegionAccessStrategyImpl implements TestRegionAccessStrategy {

      private final CachedDomainDataAccess delegate;

      public TestRegionAccessStrategyImpl(CachedDomainDataAccess delegate) {
         this.delegate = delegate;
      }

      @Override
      public SoftLock lockItem(Object session, Object key, Object version) throws CacheException {
         return delegate.lockItem(unwrap(session), key, version);
      }

      @Override
      public void unlockItem(Object session, Object key, SoftLock lock) throws CacheException {
         delegate.unlockItem(unwrap(session), key, lock);
      }

      @Override
      public boolean afterInsert(Object session, Object key, Object value, Object version) throws CacheException {
         return unwrapEntity().afterInsert(unwrap(session), key, value, version);
      }

      @Override
      public boolean afterUpdate(Object session, Object key, Object value, Object currentVersion, Object previousVersion, SoftLock lock) throws CacheException {
         return unwrapEntity().afterUpdate(unwrap(session), key, value, currentVersion, previousVersion, lock);
      }

      @Override
      public Object get(Object session, Object key) throws CacheException {
         return delegate.get(unwrap(session), key);
      }

      @Override
      public boolean putFromLoad(Object session, Object key, Object value, long txTimestamp, Object version, boolean minimalPutOverride) throws CacheException {
         return delegate.putFromLoad(unwrap(session), key, value, version, minimalPutOverride);
      }

      @Override
      public boolean putFromLoad(Object session, Object key, Object value, long txTimestamp, Object version) throws CacheException {
         return delegate.putFromLoad(unwrap(session), key, value, version);
      }

      @Override
      public void remove(Object session, Object key) throws CacheException {
         delegate.remove(unwrap(session), key);
      }

      @Override
      public boolean insert(Object session, Object key, Object value, Object version) throws CacheException {
         return unwrapEntity().insert(unwrap(session), key, value, version);
      }

      @Override
      public boolean update(Object session, Object key, Object value, Object currentVersion, Object previousVersion) throws CacheException {
         return unwrapEntity().update(unwrap(session), key, value, currentVersion, previousVersion);
      }

      @Override
      public SoftLock lockRegion() {
         return delegate.lockRegion();
      }

      @Override
      public void unlockRegion(SoftLock softLock) {
         delegate.unlockRegion(softLock);
      }

      @Override
      public void evict(Object key) {
         delegate.evict(key);
      }

      @Override
      public void evictAll() {
         delegate.evictAll();
      }

      @Override
      public void removeAll(Object session) {
         delegate.removeAll((SharedSessionContractImplementor) session);
      }

      private EntityDataAccess unwrapEntity() {
         return (EntityDataAccess) delegate;
      }

   }

   private abstract class SessionMock extends AbstractSharedSessionContract {
      public SessionMock(SessionFactoryImpl factory, SessionCreationOptions options) {
         super(factory, options);
      }
   }

   private interface NonJtaTransactionCoordinator extends TransactionCoordinatorOwner, JdbcResourceTransactionAccess {
   }

   private static final class TestRegionImpl implements TestRegion {

      private final DirectAccessRegion delegate;

      private TestRegionImpl(DirectAccessRegion delegate) {
         this.delegate = delegate;
      }

      @Override
      public Object get(Object session, Object key) throws CacheException {
         return delegate.getFromCache(key, unwrap(session));
      }

      @Override
      public void put(Object session, Object key, Object value) throws CacheException {
         delegate.putIntoCache(key, value, unwrap(session));
      }

      @Override
      public void evict(Object key) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void evictAll() {
         delegate.clear();
      }
   }

   private static class TestSynchronization implements Synchronization {
      private final Sync sync;
      private boolean registered;

      public TestSynchronization(Sync sync) {
         this.sync = sync;
      }

      @Override
      public void beforeCompletion() {
         sync.transactionCompleting();
      }

      @Override
      public void afterCompletion(int status) {
         sync.transactionCompleted(status == Status.STATUS_COMMITTING || status == Status.STATUS_COMMITTED);
      }
   }
}
