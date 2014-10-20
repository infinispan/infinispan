package org.infinispan.spring.provider;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.spring.support.embedded.InfinispanNamedEmbeddedCacheFactoryBean;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.springframework.cache.Cache;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

/**
 * <p>
 * An integration test for {@link SpringCache}.
 * </p>
 * <p>
 * <strong>CREDITS</strong> This test is a shameless copy of Costin Leau's
 * <code>org.springframework.cache.vendor.AbstractNativeCacheTest</code>. The additions made to it
 * are minor.
 * </p>
 *
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @author Marius Bogoevici
 *
 */
@Test(testName = "spring.provider.SpringCacheCacheTest", groups = "unit")
public class SpringCacheCacheTest extends SingleCacheManagerTest {

   protected final static String CACHE_NAME = "testCache";

   private final InfinispanNamedEmbeddedCacheFactoryBean<Object, Object> fb = new InfinispanNamedEmbeddedCacheFactoryBean<Object, Object>();

   private org.infinispan.Cache<Object, Object> nativeCache;

   private Cache cache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager();
   }

   @BeforeMethod
   public void setUp() throws Exception {
      this.nativeCache = createNativeCache();
      this.cache = createCache(this.nativeCache);
   }

   @AfterMethod
   public void tearDown() throws Exception {
      this.nativeCache = null;
      this.cache = null;
   }

   @Test
   public void testCacheName() throws Exception {
      assertEquals(CACHE_NAME, this.cache.getName());
   }

   @Test
   public void testNativeCache() throws Exception {
      assertSame(this.nativeCache, this.cache.getNativeCache());
   }

   @Test
   public void testCachePut() throws Exception {
      final Object key = "enescu";
      final Object value = "george";

      assertNull(this.cache.get(key));
      this.cache.put(key, value);
      assertEquals(value, this.cache.get(key).get());
   }

   @Test
   public void testCachePutSupportsNullValue() throws Exception {
      final Object key = "enescu";
      final Object value = null;

      assertNull(this.cache.get(key));
      this.cache.put(key, value);
      assertNull(this.cache.get(key).get());
   }

   @Test
   public void testCacheContains() throws Exception {
      final Object key = "enescu";
      final Object value = "george";

      this.cache.put(key, value);

      assertTrue(this.cache.get(key) != null);
   }

   @Test
   public void testCacheContainsSupportsNullValue() throws Exception {
      final Object key = "enescu";
      final Object value = null;

      this.cache.put(key, value);

      assertTrue(this.cache.get(key) != null);
   }

   @Test(expectedExceptions = NullPointerException.class)
   public void testThrowingNullPointerExceptionOnNullGet() throws Exception {
      //when
      cache.get(null);
   }

   @Test(expectedExceptions = NullPointerException.class)
   public void testThrowingNullPointerExceptionOnNullGetWithClass() throws Exception {
      //when
      cache.get(null, null);
   }

   @Test
   public void testBypassingClassCheckWhenNullIsSpecified() throws Exception {
      //given
      this.cache.put("test", "test");

      //when
      Object value = cache.get("test", null);

      //then
      assertTrue(value instanceof String);
   }

   @Test
   public void testReturningProperValueFromCacheWithCast() throws Exception {
      //given
      this.cache.put("test", "test");

      //when
      String value = cache.get("test", String.class);

      //then
      assertEquals("test", value);
   }

   @Test(expectedExceptions = IllegalStateException.class)
   public void testThrowingIllegalStateExceptionWhenClassCastReturnDifferentClass() throws Exception {
      //given
      this.cache.put("test", new Object());

      //when
      cache.get("test", String.class);
   }

   @Test
   public void testReturningNullValueIfThereIsNoValue() throws Exception {
      //when
      Cache.ValueWrapper existingValue = this.cache.putIfAbsent("test", "test");

      //then
      assertNull(existingValue);
   }

   @Test
   public void testReturningPreviousValue() throws Exception {
      //given
      this.cache.put("test", "test");

      //when
      Cache.ValueWrapper existingValue = this.cache.putIfAbsent("test", "test1");

      //then
      assertEquals("test", existingValue.get());
   }

   @Test
   public void testReturningNullValueConstant() throws Exception {
      //given
      this.cache.put("test", NullValue.NULL);

      //when
      Cache.ValueWrapper existingValue = this.cache.putIfAbsent("test", "test1");

      //then
      assertEquals(NullValue.NULL, existingValue);
   }

   @Test
   public void testCacheClear() throws Exception {
      assertNull(this.cache.get("enescu"));
      this.cache.put("enescu", "george");
      assertNull(this.cache.get("vlaicu"));
      this.cache.put("vlaicu", "aurel");
      this.cache.clear();
      assertNull(this.cache.get("vlaicu"));
      assertNull(this.cache.get("enescu"));
   }

   private org.infinispan.Cache<Object, Object> createNativeCache() throws Exception {
      this.fb.setInfinispanEmbeddedCacheManager(cacheManager);
      this.fb.setBeanName(CACHE_NAME);
      this.fb.setCacheName(CACHE_NAME);
      this.fb.afterPropertiesSet();
      return this.fb.getObject();
   }

   private Cache createCache(final org.infinispan.Cache<Object, Object> nativeCache) {
      return new SpringCache(nativeCache);
   }

}
