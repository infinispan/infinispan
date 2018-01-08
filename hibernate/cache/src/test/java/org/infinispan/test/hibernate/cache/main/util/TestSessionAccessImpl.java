package org.infinispan.test.hibernate.cache.main.util;

import org.hibernate.FlushMode;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.GeneralDataRegion;
import org.hibernate.cache.spi.access.EntityRegionAccessStrategy;
import org.hibernate.cache.spi.access.RegionAccessStrategy;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.jdbc.connections.spi.JdbcConnectionAccess;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.transaction.internal.TransactionImpl;
import org.hibernate.engine.transaction.jta.platform.spi.JtaPlatform;
import org.hibernate.query.Query;
import org.hibernate.resource.jdbc.spi.JdbcSessionContext;
import org.hibernate.resource.jdbc.spi.JdbcSessionOwner;
import org.hibernate.resource.transaction.backend.jdbc.internal.JdbcResourceLocalTransactionCoordinatorBuilderImpl;
import org.hibernate.resource.transaction.backend.jdbc.spi.JdbcResourceTransactionAccess;
import org.hibernate.resource.transaction.spi.TransactionCoordinator;
import org.hibernate.resource.transaction.spi.TransactionCoordinatorOwner;
import org.hibernate.service.ServiceRegistry;
import org.infinispan.test.hibernate.cache.commons.util.BatchModeJtaPlatform;
import org.infinispan.test.hibernate.cache.commons.util.JdbcResourceTransactionMock;
import org.infinispan.test.hibernate.cache.commons.util.TestSessionAccess;
import org.infinispan.util.ControlledTimeService;
import org.kohsuke.MetaInfServices;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@MetaInfServices(TestSessionAccess.class)
public class TestSessionAccessImpl implements TestSessionAccess {

   @Override
   public Object mockSessionImplementor() {
      return mock(SharedSessionContractImplementor.class);
   }

   @Override
   public Object mockSession(Class<? extends JtaPlatform> jtaPlatform, ControlledTimeService timeService) {
      SessionMock session = mock(SessionMock.class);
      when(session.isClosed()).thenReturn(false);
      when(session.getTimestamp()).thenReturn(timeService.wallClockTime());
      if (jtaPlatform == BatchModeJtaPlatform.class) {
         BatchModeTransactionCoordinator txCoord = new BatchModeTransactionCoordinator();
         when(session.getTransactionCoordinator()).thenReturn(txCoord);
         when(session.beginTransaction()).then(invocation -> {
            Transaction tx = txCoord.newTransaction();
            tx.begin();
            return tx;
         });
      } else if (jtaPlatform == null) {
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
         ServiceRegistry serviceRegistry = mock(ServiceRegistry.class);
         when(serviceRegistry.getService(JdbcServices.class)).thenReturn(jdbcServices);
         JdbcSessionContext jdbcSessionContext = mock(JdbcSessionContext.class);
         when(jdbcSessionContext.getServiceRegistry()).thenReturn(serviceRegistry);
         when(jdbcSessionOwner.getJdbcSessionContext()).thenReturn(jdbcSessionContext);
         NonJtaTransactionCoordinator txOwner = mock(NonJtaTransactionCoordinator.class);
         when(txOwner.getResourceLocalTransaction()).thenReturn(new JdbcResourceTransactionMock());
         when(txOwner.getJdbcSessionOwner()).thenReturn(jdbcSessionOwner);
         when(txOwner.isActive()).thenReturn(true);
         TransactionCoordinator txCoord = JdbcResourceLocalTransactionCoordinatorBuilderImpl.INSTANCE
            .buildTransactionCoordinator(txOwner, null);
         when(session.getTransactionCoordinator()).thenReturn(txCoord);
         when(session.beginTransaction()).then(invocation -> {
            Transaction tx = new TransactionImpl(txCoord, session.getExceptionConverter());
            tx.begin();
            return tx;
         });
      } else {
         throw new IllegalStateException("Unknown JtaPlatform: " + jtaPlatform);
      }
      return session;
   }

   @Override
   public Transaction beginTransaction(Object session) {
      return ((Session) session).beginTransaction();
   }

   @Override
   public TestRegionAccessStrategy fromAccessStrategy(RegionAccessStrategy strategy) {
      return new TestRegionAccessStrategyImpl(strategy);
   }

   @Override
   public TestGeneralDataRegion fromGeneralDataRegion(GeneralDataRegion region) {
      return new TestGeneralDataRegionImpl(region);
   }

   @Override
   public List execQueryList(Object session, String query, String[]... params) {
      Query q = ((Session) session).createQuery(query);
      setParams(q, params);
      return q.list();
   }

   @Override
   public List execQueryListAutoFlush(Object session, String query, String[]... params) {
      Query q = ((Session) session).createQuery(query).setFlushMode(FlushMode.AUTO);
      setParams(q, params);
      return q.list();
   }

   @Override
   public List execQueryListCacheable(Object session, String query) {
      return ((Session) session).createQuery(query).setCacheable(true).list();
   }

   @Override
   public int execQueryUpdateAutoFlush(Object session, String query, String[]... params) {
      Query q = ((Session) session).createQuery(query).setFlushMode(FlushMode.AUTO);
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

   private static SharedSessionContractImplementor unwrap(Object session) {
      return (SharedSessionContractImplementor) session;
   }

   private static final class TestRegionAccessStrategyImpl implements TestRegionAccessStrategy {

      private final RegionAccessStrategy delegate;

      public TestRegionAccessStrategyImpl(RegionAccessStrategy delegate) {
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
      public Object get(Object session, Object key, long txTimestamp) throws CacheException {
         return delegate.get(unwrap(session), key, txTimestamp);
      }

      @Override
      public boolean putFromLoad(Object session, Object key, Object value, long txTimestamp, Object version, boolean minimalPutOverride) throws CacheException {
         return delegate.putFromLoad(unwrap(session), key, value, txTimestamp, version, minimalPutOverride);
      }

      @Override
      public boolean putFromLoad(Object session, Object key, Object value, long txTimestamp, Object version) throws CacheException {
         return delegate.putFromLoad(unwrap(session), key, value, txTimestamp, version);
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

      private EntityRegionAccessStrategy unwrapEntity() {
         return (EntityRegionAccessStrategy) delegate;
      }

   }

   private interface SessionMock extends Session, SharedSessionContractImplementor {
   }

   private interface NonJtaTransactionCoordinator extends TransactionCoordinatorOwner, JdbcResourceTransactionAccess {
   }

   private static final class TestGeneralDataRegionImpl implements TestGeneralDataRegion {

      private final GeneralDataRegion delegate;

      private TestGeneralDataRegionImpl(GeneralDataRegion delegate) {
         this.delegate = delegate;
      }

      @Override
      public Object get(Object session, Object key) throws CacheException {
         return delegate.get(unwrap(session), key);
      }

      @Override
      public void put(Object session, Object key, Object value) throws CacheException {
         delegate.put(unwrap(session), key, value);
      }

   }

}
