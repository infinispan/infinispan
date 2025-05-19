package org.infinispan.spring.embedded.provider;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.NullValue;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.spring.embedded.support.InfinispanNamedEmbeddedCacheFactoryBean;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.springframework.cache.Cache;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;

/**
 * <p>
 * An integration test for {@link SpringCache}.
 * </p>
 * <p>
 * <b>CREDITS</b> This test is a shameless copy of Costin Leau's
 * <code>org.springframework.cache.vendor.AbstractNativeCacheTest</code>. The additions made to it
 * are minor.
 * </p>
 *
 * @author Olaf Bergner
 * @author Marius Bogoevici
 *
 */
@Test(testName = "spring.embedded.provider.SpringCacheTest", groups = "unit")
public class SpringCacheTest extends SingleCacheManagerTest {

   protected static final String CACHE_NAME = "testCache";

   private final InfinispanNamedEmbeddedCacheFactoryBean<Object, Object> fb = new InfinispanNamedEmbeddedCacheFactoryBean<Object, Object>();

   private final String mediaType;

   private org.infinispan.Cache<Object, Object> nativeCache;

   private Cache cache;

   @Factory(dataProvider = "encodings")
   public SpringCacheTest(String mediaType) {
      this.mediaType = mediaType;
   }

   @DataProvider
   public static Object[][] encodings() {
      return new Object[][] {
            {MediaType.APPLICATION_OBJECT_TYPE},
            {MediaType.APPLICATION_SERIALIZED_OBJECT_TYPE},
            {MediaType.APPLICATION_PROTOSTREAM_TYPE},
      };
   }

   @Override
   protected String parameters() {
      return "[" + mediaType + "]";
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder defaultCacheBuilder = new ConfigurationBuilder();
      defaultCacheBuilder.encoding().mediaType(mediaType);

      return TestCacheManagerFactory.createCacheManager(
            new GlobalConfigurationBuilder().defaultCacheName(CACHE_NAME),
            defaultCacheBuilder
      );
   }

   @BeforeMethod
   public void setUp() throws Exception {
      this.nativeCache = createNativeCache();
      this.cache = createCache(this.nativeCache);
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
      cache.get(null, (Class<?>) null);
   }

   @Test
   public void testBypassingClassCheckWhenNullIsSpecified() throws Exception {
      //given
      this.cache.put("test", "test");

      //when
      Object value = cache.get("test", (Class<?>) null);

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
      this.cache.put("test", 1);

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
      assertNotNull(existingValue);
      assertNull(existingValue.get());
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

   @Test
   public void testValueLoaderWithNoPreviousValue() {
      //given
      cache.get("test", () -> "test");

      //when
      Cache.ValueWrapper valueFromCache = cache.get("test");

      //then
      assertEquals("test", valueFromCache.get());
   }

   /*
    * In this test Thread 1 should exclusively block Cache#get method so that Thread 2 won't be able to
    * insert "thread2" string into the cache.
    *
    * The test check this part of the Spring spec:
    * Return the value to which this cache maps the specified key, obtaining that value from valueLoader if necessary.
    * This method provides a simple substitute for the conventional "if cached, return; otherwise create, cache and return" pattern.
    * @see http://docs.spring.io/spring/docs/current/javadoc-api/org/springframework/cache/Cache.html#get-java.lang.Object-java.util.concurrent.Callable-
    */
   @Test
   public void testValueLoaderWithLocking() throws Exception {
      //given
      CountDownLatch waitUntilThread1LocksValueGetter = new CountDownLatch(1);

      //when
      Future<String> thread1 = fork(() -> cache.get("test", () -> {
         waitUntilThread1LocksValueGetter.countDown();
         return "thread1";
      }));

      Future<String> thread2 = fork(() -> {
         waitUntilThread1LocksValueGetter.await(30, TimeUnit.SECONDS);
         return cache.get("test", () -> "thread2");
      });

      String valueObtainedByThread1 = thread1.get();
      String valueObtainedByThread2 = thread2.get();

      Cache.ValueWrapper valueAfterGetterIsDone = cache.get("test");

      //then
      assertNotNull(valueAfterGetterIsDone);
      assertEquals("thread1", valueAfterGetterIsDone.get());
      assertEquals("thread1", valueObtainedByThread1);
      assertEquals("thread1", valueObtainedByThread2);
   }

   @Test
   public void testValueLoaderWithPreviousValue() {
      //given
      cache.put("test", "test");
      cache.get("test", () -> "This should not be updated");

      //when
      Cache.ValueWrapper valueFromCache = cache.get("test");

      //then
      assertEquals("test", valueFromCache.get());
   }

   @Test(expectedExceptions = Cache.ValueRetrievalException.class)
   public void testValueLoaderWithExceptionWhenLoading() {
      //when//then
      cache.get("test", () -> {throw new IllegalStateException();});
   }

   @Test
   public void testPutNullAndGetWithClass() {
      //when//then
      cache.put("key", null);

      assertNull(cache.get("key", String.class));
   }

   @Test
   public void testGetWithNullValue() {
      assertNull(cache.get("null", () -> null));
   }

   @Test
   public void testGetNullValueAfterPutNull() {
      cache.put("key", null);

      String result = cache.get("key", () -> "notnull");

      assertNull(result);
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
