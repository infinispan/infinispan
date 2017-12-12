package org.infinispan.test.hibernate.cache.commons.util;

import org.hibernate.Transaction;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.GeneralDataRegion;
import org.hibernate.cache.spi.access.RegionAccessStrategy;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.transaction.jta.platform.spi.JtaPlatform;
import org.infinispan.util.ControlledTimeService;

import java.util.List;
import java.util.ServiceLoader;

public interface TestSessionAccess {

   Object mockSessionImplementor();

   Object mockSession(Class<? extends JtaPlatform> jtaPlatform, ControlledTimeService timeService);

   Transaction beginTransaction(Object session);

   TestRegionAccessStrategy fromAccessStrategy(RegionAccessStrategy strategy);

   TestGeneralDataRegion fromGeneralDataRegion(GeneralDataRegion region);

   List execQueryList(Object session, String query, String[]... params);

   List execQueryListAutoFlush(Object session, String query, String[]... params);

   List execQueryListCacheable(Object session, String query);

   int execQueryUpdateAutoFlush(Object session, String query, String[]... params);

   void execQueryUpdate(Object session, String query);

   static TestSessionAccess findTestSessionAccess() {
      ServiceLoader<TestSessionAccess> loader = ServiceLoader.load(TestSessionAccess.class);
      return loader.iterator().next();
   }

   interface TestRegionAccessStrategy {

      SoftLock lockItem(Object session, Object key, Object version) throws CacheException;

      void unlockItem(Object session, Object key, SoftLock lock) throws CacheException;

      boolean afterInsert(Object session, Object key, Object value, Object version) throws CacheException;

      boolean afterUpdate(Object session, Object key, Object value, Object currentVersion, Object previousVersion, SoftLock lock) throws CacheException;

      Object get(Object session, Object key, long txTimestamp) throws CacheException;

      boolean putFromLoad(Object session, Object key, Object value, long txTimestamp, Object version, boolean minimalPutOverride) throws CacheException;

      boolean putFromLoad(Object session, Object key, Object value, long txTimestamp, Object version) throws CacheException;

      void remove(Object session, Object key) throws CacheException;

      boolean insert(Object session, Object key, Object value, Object version) throws CacheException;

      boolean update(Object session, Object key, Object value, Object currentVersion, Object previousVersion) throws CacheException;

   }

   interface TestGeneralDataRegion {

      Object get(Object session, Object key) throws CacheException;

      void put(Object session, Object key, Object value) throws CacheException;

   }

}
