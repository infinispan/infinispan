package org.infinispan.test;

import static java.util.Arrays.asList;
import static org.infinispan.test.fwk.TestResourceTracker.getCurrentTestShortName;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.BiasAcquisition;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.container.DataContainer;
import org.infinispan.distribution.MagicKey;
import org.infinispan.distribution.rehash.XAResourceAdapter;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.fwk.InCacheMode;
import org.infinispan.test.fwk.InTransactionMode;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestFrameworkFailure;
import org.infinispan.test.fwk.TestResourceTracker;
import org.infinispan.test.fwk.TestSelector;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.IMethodInstance;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;


/**
 * Base class for tests that operates on clusters of caches. The way tests extending this class operates is:
 * <pre>
 *    1) created cache managers before tests start. The cache managers are only created once
 *    2) after each test method runs, the cache instances are being cleared
 *    3) next test method will run on same cacheManager instance. This way the test is much faster, as CacheManagers
 *       are expensive to create.
 * </pre>
 * If, however, you would like your cache managers destroyed after every <i>test method</i> instead of the </i>test
 * class</i>, you could set the <tt>cleanup</tt> field to {@link MultipleCacheManagersTest.CleanupPhase#AFTER_METHOD} in
 * your test's constructor.  E.g.:
 * <pre>
 * public void MyTest extends MultipleCacheManagersTest {
 *    public MyTest() {
 *       cleanup =  CleanupPhase.AFTER_METHOD;
 *    }
 * }
 * </pre>
 * <p>
 * Note that this will cause {@link #createCacheManagers()}  to be called before each method.
 *
 * @author Mircea.Markus@jboss.com
 */
@TestSelector(filters = {
      MultipleCacheManagersTest.CacheModeFilter.class,
      MultipleCacheManagersTest.TransactionalModeFilter.class,
      MultipleCacheManagersTest.TotalOrderFilter.class,
      MultipleCacheManagersTest.LockingModeFilter.class,
      MultipleCacheManagersTest.IsolationLevelFilter.class,
})
public abstract class MultipleCacheManagersTest extends AbstractCacheTest {

   protected List<EmbeddedCacheManager> cacheManagers = Collections.synchronizedList(new ArrayList<>());
   protected IdentityHashMap<Cache<?, ?>, ReplListener> listeners = new IdentityHashMap<>();
   // the cache mode set in configuration is shared in many tests, therefore we'll place the field,
   // fluent setter cacheMode(...) and parameters() to this class.
   protected CacheMode cacheMode;
   protected Boolean transactional;
   protected LockingMode lockingMode;
   protected Boolean totalOrder;
   protected BiasAcquisition biasAcquisition;
   protected IsolationLevel isolationLevel;
   // Disables the triangle algorithm if set to Boolean.FALSE
   protected Boolean useTriangle;
   private boolean parametrizedInstance = false;

   @BeforeClass(alwaysRun = true)
   public void createBeforeClass() throws Throwable {
      checkFactoryAnnotation();
      if (cleanupAfterTest()) callCreateCacheManagers();
   }

   private void callCreateCacheManagers() throws Throwable {
      try {
         log.debug("Creating cache managers");
         createCacheManagers();
         log.debug("Cache managers created, ready to start the test");
      } catch (Throwable th) {
         log.error("Error in test setup: ", th);
         throw th;
      }
   }

   @BeforeMethod(alwaysRun = true)
   public void createBeforeMethod() throws Throwable {
      if (cleanupAfterMethod()) callCreateCacheManagers();
   }

   @AfterClass(alwaysRun = true)
   protected void destroy() {
      if (cleanupAfterTest()) {
         TestingUtil.clearContent(cacheManagers);
         TestingUtil.killCacheManagers(cacheManagers);
      }

      for (EmbeddedCacheManager cm : cacheManagers) {
         String nodeName = cm.getCacheManagerConfiguration().transport().nodeName();
         assertTrue("Invalid node name for test " + getCurrentTestShortName() + ": " + nodeName,
                    nodeName != null && nodeName.contains(getCurrentTestShortName()));
      }

      cacheManagers.clear();
      listeners.clear();
   }

   @AfterMethod(alwaysRun = true)
   protected void clearContent() throws Throwable {
      if (cleanupAfterTest()) {
         log.debug("*** Test method complete; clearing contents on all caches.");
         TestingUtil.clearContent(cacheManagers);
      } else {
         TestingUtil.clearContent(cacheManagers);
         TestingUtil.killCacheManagers(cacheManagers);
         TestResourceTracker.cleanUpResources(getTestName());
         cacheManagers.clear();
      }
   }

   final protected void registerCacheManager(CacheContainer... cacheContainers) {
      for (CacheContainer ecm : cacheContainers) {
         this.cacheManagers.add((EmbeddedCacheManager) ecm);
      }
   }

   /**
    * Creates a new cache manager, starts it, and adds it to the list of known cache managers on the current thread.
    * Uses a default clustered cache manager global config.
    *
    * @return the new CacheManager
    */
   protected EmbeddedCacheManager addClusterEnabledCacheManager() {
      return addClusterEnabledCacheManager(new TransportFlags());
   }

   /**
    * Creates a new cache manager, starts it, and adds it to the list of known
    * cache managers on the current thread. Uses a default clustered cache
    * manager global config.
    *
    * @param flags properties that allow transport stack to be tweaked
    * @return the new CacheManager
    */
   protected EmbeddedCacheManager addClusterEnabledCacheManager(TransportFlags flags) {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(false,
            GlobalConfigurationBuilder.defaultClusteredBuilder(), new ConfigurationBuilder(), flags, false);
      amendCacheManagerBeforeStart(cm);
      cacheManagers.add(cm);
      cm.start();
      return cm;
   }

   /**
    * Allows a test to manipulate a cache manager before it is started. Does nothing by default
    * @param cm
    */
   protected void amendCacheManagerBeforeStart(EmbeddedCacheManager cm) {
      // Do nothing
   }

   /**
    * Creates a new non-transactional cache manager, starts it, and adds it to the list of known cache managers on the
    * current thread.  Uses a default clustered cache manager global config.
    *
    * @param defaultConfig default cfg to use
    * @return the new CacheManager
    */
   protected EmbeddedCacheManager addClusterEnabledCacheManager(ConfigurationBuilder defaultConfig) {
      return addClusterEnabledCacheManager(defaultConfig, new TransportFlags());
   }

   protected EmbeddedCacheManager addClusterEnabledCacheManager(GlobalConfigurationBuilder globalBuilder, ConfigurationBuilder defaultConfig) {
      return addClusterEnabledCacheManager(globalBuilder, defaultConfig, new TransportFlags());
   }

   /**
    * Creates a new optionally transactional cache manager, starts it, and adds it to the list of known cache managers on
    * the current thread.  Uses a default clustered cache manager global config.
    *
    * @param builder default cfg to use
    * @return the new CacheManager
    */
   protected EmbeddedCacheManager addClusterEnabledCacheManager(ConfigurationBuilder builder, TransportFlags flags) {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(false, GlobalConfigurationBuilder.defaultClusteredBuilder(), builder, flags, false);
      amendCacheManagerBeforeStart(cm);
      cacheManagers.add(cm);
      cm.start();
      return cm;
   }

   protected EmbeddedCacheManager addClusterEnabledCacheManager(ConfigurationBuilderHolder builderHolder) {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(false, builderHolder, false);
      amendCacheManagerBeforeStart(cm);
      cacheManagers.add(cm);
      cm.start();
      return cm;
   }

   protected EmbeddedCacheManager addClusterEnabledCacheManager(GlobalConfigurationBuilder globalBuilder, ConfigurationBuilder builder, TransportFlags flags) {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(false, globalBuilder, builder, flags, false);
      amendCacheManagerBeforeStart(cm);
      cacheManagers.add(cm);
      cm.start();
      return cm;
   }

   protected void createCluster(ConfigurationBuilder builder, int count) {
      for (int i = 0; i < count; i++) addClusterEnabledCacheManager(builder);
   }

   /**
    * Allows multiple configurations to be defined for a cache manager before it is started, using the supplied
    * {@link ConfigurationBuilderHolder}.  These cannot be shared per node so this method doesn't allow the user to make
    * the mistake and instead will give you one instance per node.
    * <p>
    * This method will wait until all nodes are up before returning
    * @param consumer consumer to configure the caches
    * @param count how many nodes to bring up
    */
   protected void createCluster(Consumer<ConfigurationBuilderHolder> consumer, int count) {
      for (int i = 0; i < count; ++i) {
         ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
         holder.getGlobalConfigurationBuilder().clusteredDefault();
         consumer.accept(holder);
         addClusterEnabledCacheManager(holder);
      }
      waitForClusterToForm();
   }

   protected void createCluster(GlobalConfigurationBuilder globalBuilder, ConfigurationBuilder builder, int count) {
      for (int i = 0; i < count; i++)
         addClusterEnabledCacheManager(new GlobalConfigurationBuilder().read(globalBuilder.build()), builder);
   }

   protected void defineConfigurationOnAllManagers(String cacheName, ConfigurationBuilder b) {
      for (EmbeddedCacheManager cm : cacheManagers) {
         cm.defineConfiguration(cacheName, b.build());
      }
   }

   protected <K, V> List<Cache<K, V>> getCaches(String cacheName) {
      List<Cache<K, V>> caches = new ArrayList<>();
      List<EmbeddedCacheManager> managers = new ArrayList<>(cacheManagers);
      for (EmbeddedCacheManager cm : managers) {
         Cache<K, V> c;
         if (cacheName == null)
            c = cm.getCache();
         else
            c = cm.getCache(cacheName);
         caches.add(c);
      }
      return caches;
   }

   protected void waitForClusterToForm(String cacheName) {
      List<Cache<Object, Object>> caches = getCaches(cacheName);
      Cache<Object, Object> cache = caches.get(0);
      TestingUtil.blockUntilViewsReceived(30000, caches);
      if (cache.getCacheConfiguration().clustering().cacheMode().isClustered()) {
         TestingUtil.waitForNoRebalance(caches);
      }
   }

   protected void waitForClusterToForm() {
      waitForClusterToForm((String) null);
   }

   protected void waitForClusterToForm(String... names) {
      if (names != null && names.length != 0) {
         for (String name : names) {
            waitForClusterToForm(name);
         }
      } else {
         waitForClusterToForm();
      }
   }

   protected TransactionManager tm(Cache<?, ?> c) {
      return c.getAdvancedCache().getTransactionManager();
   }

   protected TransactionManager tm(int i, String cacheName) {
      return cache(i, cacheName).getAdvancedCache().getTransactionManager();
   }

   protected TransactionManager tm(int i) {
      return cache(i).getAdvancedCache().getTransactionManager();
   }

   protected Transaction tx(int i) {
      try {
         return cache(i).getAdvancedCache().getTransactionManager().getTransaction();
      } catch (SystemException e) {
         throw new RuntimeException(e);
      }
   }

   protected <K, V> List<Cache<K, V>> createClusteredCaches(
         int numMembersInCluster, String cacheName, ConfigurationBuilder builder) {
      return createClusteredCaches(numMembersInCluster, cacheName, builder, new TransportFlags());
   }

   protected <K, V> List<Cache<K, V>> createClusteredCaches(
         int numMembersInCluster, String cacheName, ConfigurationBuilder builder, TransportFlags flags) {
      List<Cache<K, V>> caches = new ArrayList<>(numMembersInCluster);
      for (int i = 0; i < numMembersInCluster; i++) {
         EmbeddedCacheManager cm = addClusterEnabledCacheManager(flags);
         cm.defineConfiguration(cacheName, builder.build());
         Cache<K, V> cache = cm.getCache(cacheName);
         caches.add(cache);
      }
      waitForClusterToForm(cacheName);
      return caches;
   }

   protected <K, V> List<Cache<K, V>> createClusteredCaches(int numMembersInCluster,
                                                            GlobalConfigurationBuilder globalConfigurationBuilder,
                                                            ConfigurationBuilder defaultConfigBuilder,
                                                            boolean serverMode, String... cacheNames) {
      List<Cache<K, V>> caches = new ArrayList<>(numMembersInCluster);
      for (int i = 0; i < numMembersInCluster; i++) {
         EmbeddedCacheManager cm;
         if (serverMode) {
            globalConfigurationBuilder.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
            globalConfigurationBuilder.transport().defaultTransport();
            cm = addClusterEnabledCacheManager(globalConfigurationBuilder, defaultConfigBuilder);
         } else {
            cm = addClusterEnabledCacheManager(defaultConfigBuilder);
         }
         if (cacheNames.length == 0) {
            Cache<K, V> cache = cm.getCache();
            caches.add(cache);
         } else {
            for (String cacheName : cacheNames) {
               cm.defineConfiguration(cacheName, defaultConfigBuilder.build());
               caches.add(cm.getCache(cacheName));
            }
         }
      }
      waitForClusterToForm(cacheNames);
      return caches;
   }

   protected <K, V> List<Cache<K, V>> createClusteredCaches(int numMembersInCluster,
                                                            ConfigurationBuilder defaultConfigBuilder,
                                                            boolean serverMode, String... cacheNames) {
      return createClusteredCaches(numMembersInCluster, new GlobalConfigurationBuilder(), defaultConfigBuilder, serverMode, cacheNames);
   }

   protected <K, V> List<Cache<K, V>> createClusteredCaches(int numMembersInCluster,
                                                            ConfigurationBuilder defaultConfigBuilder) {
      return createClusteredCaches(numMembersInCluster, defaultConfigBuilder, false);
   }

   protected <K, V> List<Cache<K, V>> createClusteredCaches(int numMembersInCluster,
                                                            ConfigurationBuilder defaultConfig,
                                                            TransportFlags flags) {
      List<Cache<K, V>> caches = new ArrayList<>(numMembersInCluster);
      for (int i = 0; i < numMembersInCluster; i++) {
         EmbeddedCacheManager cm = addClusterEnabledCacheManager(defaultConfig, flags);
         Cache<K, V> cache = cm.getCache();
         caches.add(cache);
      }
      waitForClusterToForm();
      return caches;
   }

   /**
    * Create cacheNames.length in each CacheManager (numMembersInCluster cacheManagers).
    *
    * @param numMembersInCluster
    * @param defaultConfigBuilder
    * @param cacheNames
    * @return A list with size numMembersInCluster containing a list of cacheNames.length caches
    */
   protected <K, V> List<List<Cache<K, V>>> createClusteredCaches(int numMembersInCluster,
                                                                  ConfigurationBuilder defaultConfigBuilder, String... cacheNames) {
      return createClusteredCaches(numMembersInCluster, defaultConfigBuilder, new TransportFlags(), cacheNames);
   }

   protected <K, V> List<List<Cache<K, V>>> createClusteredCaches(int numMembersInCluster,
                                                                  ConfigurationBuilder defaultConfigBuilder, TransportFlags transportFlags, String... cacheNames) {
      List<List<Cache<K, V>>> allCaches = new ArrayList<>(numMembersInCluster);
      for (int i = 0; i < numMembersInCluster; i++) {
         EmbeddedCacheManager cm = addClusterEnabledCacheManager(defaultConfigBuilder, transportFlags);
         List<Cache<K, V>> currentCacheManagerCaches = new ArrayList<>(cacheNames.length);

         for (String cacheName : cacheNames) {
            cm.defineConfiguration(cacheName, defaultConfigBuilder.build());
            Cache<K, V> cache = cm.getCache(cacheName);
            currentCacheManagerCaches.add(cache);
         }
         allCaches.add(currentCacheManagerCaches);
      }
      waitForClusterToForm(cacheNames);
      return allCaches;
   }

   protected ReplListener replListener(Cache<?, ?> cache) {
      return listeners.computeIfAbsent(cache, k -> new ReplListener(cache));
   }

   protected EmbeddedCacheManager[] managers() {
      return cacheManagers.toArray(new EmbeddedCacheManager[cacheManagers.size()]);
   }

   protected EmbeddedCacheManager manager(int i) {
      return cacheManagers.get(i);
   }

   public EmbeddedCacheManager manager(Address a) {
      for (EmbeddedCacheManager cm : cacheManagers) {
         if (cm.getAddress().equals(a)) {
            return cm;
         }
      }
      throw new IllegalArgumentException(a + " is not a valid cache manager address!");
   }

   public int managerIndex(Address a) {
      for (int i = 0; i < cacheManagers.size(); i++) {
         EmbeddedCacheManager cm = cacheManagers.get(i);
         if (cm.getAddress().equals(a)) {
            return i;
         }
      }
      throw new IllegalArgumentException(a + " is not a valid cache manager address!");
   }

   protected <K, V> Cache<K, V> cache(int managerIndex, String cacheName) {
      return manager(managerIndex).getCache(cacheName);
   }

   protected void assertClusterSize(String message, int size) {
      for (EmbeddedCacheManager cm : cacheManagers) {
         assert cm.getMembers() != null && cm.getMembers().size() == size : message;
      }
   }

   protected void removeCacheFromCluster(String cacheName) {
      for (EmbeddedCacheManager cm : cacheManagers) {
         TestingUtil.killCaches(cm.getCache(cacheName));
      }
   }

   /**
    * Returns the default cache from that manager.
    */
   protected <A, B> Cache<A, B> cache(int index) {
      return manager(index).getCache();
   }

   protected DataContainer dataContainer(int index) {
      return advancedCache(index).getDataContainer();
   }

   /**
    * This is the method you should override when providing factory method.
    */
   public Object[] factory() {
      throw new IllegalStateException("Only overridden methods should be called!");
   }

   @Factory
   public Object[] defaultFactory() {
      // It is possible to override the factory method, but if we extend a class that defines such overridden
      // method, the factory method will be inherited, too - that results in running the superclass tests
      // instead of current class tests.
      try {
         Method factory = getClass().getMethod("factory");
         if (factory.getDeclaringClass() == getClass()) {
            Object[] instances = factory();
            for (int i = 0; i < instances.length; i++) {
               if (instances[i].getClass() != getClass()) {
                  instances[i] = new TestFrameworkFailure("%s.factory() creates instances of %s",
                                                          getClass().getName(), instances[i].getClass().getName());
               }
            }
            return instances;
         } else if (factory.getDeclaringClass() != MultipleCacheManagersTest.class) {
            return new Object[]{new TestFrameworkFailure(
               "%s.factory() override is missing, inherited factory() creates instances of %s",
               getClass().getName(), factory.getDeclaringClass().getName())};
         }
      } catch (NoSuchMethodException e) {
         throw new IllegalStateException("Every class should have factory method, at least inherited", e);
      }
      List<Consumer<MultipleCacheManagersTest>[]> allModifiers;
      try {
         Consumer<MultipleCacheManagersTest>[] cacheModeModifiers =
               getModifiers(InCacheMode.class, InCacheMode::value, MultipleCacheManagersTest::cacheMode);
         Consumer<MultipleCacheManagersTest>[] transactionModifiers =
               getModifiers(InTransactionMode.class, InTransactionMode::value,
                     (t, m) -> t.transactional(m.isTransactional()));
         allModifiers = asList(cacheModeModifiers, transactionModifiers);
      } catch (Exception e) {
         return new Object[]{new TestFrameworkFailure(e)};
      }

      int numTests = allModifiers.stream().mapToInt(m -> m.length).reduce(1, (m1, m2) -> m1 * m2);
      Object[] tests = new Object[numTests];
      tests[0] = this;
      Constructor<? extends MultipleCacheManagersTest> ctor;
      try {
         ctor = getClass().getConstructor();
      } catch (NoSuchMethodException e) {
         return new Object[]{new TestFrameworkFailure("Missing no-arg constructor in %s", getClass().getName())};
      }
      for (int i = 1; i < tests.length; ++i) {
         try {
            tests[i] = ctor.newInstance();
         } catch (Exception e) {
            return new Object[]{new TestFrameworkFailure(e)};
         }
      }
      int stride = 1;
      for (Consumer<MultipleCacheManagersTest>[] modifiers : allModifiers) {
         applyModifiers(tests, modifiers, stride);
         stride *= modifiers.length;
      }
      return tests;
   }

   private void checkFactoryAnnotation() {
      for (Method m : getClass().getMethods()) {
         if (m.getAnnotation(Factory.class) != null && m.getDeclaringClass() != MultipleCacheManagersTest.class) {
            throw new IllegalStateException("Test " + getClass().getName() +
                  " extends MultipleCacheManagersTest and declares its own @Factory method: " +
                  m.getName());
         }
      }
   }

   private void applyModifiers(Object[] tests, Consumer<MultipleCacheManagersTest>[] modifiers, int stride) {
      for (int i = 0, mi = 0; i < tests.length; i += stride, mi = (mi + 1) % modifiers.length) {
         for (int j = 0; j < stride; ++j) {
            modifiers[mi].accept((MultipleCacheManagersTest) tests[i + j]);
         }
      }
   }

   private <Mode, A extends Annotation> Consumer<MultipleCacheManagersTest>[] getModifiers(Class<A> annotationClass, Function<A, Mode[]> methodRetriever, BiConsumer<MultipleCacheManagersTest, Mode> applier) {
      Mode[] classModes = classModes(annotationClass, methodRetriever);
      Set<Mode> methodModes = methodModes(annotationClass, methodRetriever);
      if (classModes == null && methodModes == null) {
         return new Consumer[]{t -> {
         }}; // no modifications
      }
      Set<Mode> allModes = new HashSet<>();
      if (classModes != null) {
         allModes.addAll(asList(classModes));
      }
      if (methodModes != null && !allModes.containsAll(methodModes)) {
         throw new IllegalStateException(
               "Test methods cannot declare cache mode/transaction filters that the test class hasn't declared");
      }
      // if there are only method-level annotations, add a version without setting mode at all
      return allModes.stream()
            .map(mode -> (Consumer<MultipleCacheManagersTest>) t -> applier.accept(t, mode))
            .toArray(Consumer[]::new);
   }

   protected <Mode, A extends Annotation> Set<Mode> methodModes(Class<A> annotationClass, Function<A, Mode[]> modeRetriever) {
      // the annotation is not inherited
      Set<Mode> modes = null;
      for (Method m : getClass().getMethods()) {
         A annotation = m.getAnnotation(annotationClass);
         if (annotation == null) continue;
         if (modes == null) {
            modes = new HashSet<>();
         }
         Collections.addAll(modes, modeRetriever.apply(annotation));
      }
      return modes;
   }

   protected <Mode, A extends Annotation> Mode[] classModes(Class<A> annotationClass, Function<A, Mode[]> modeRetriever) {
      A annotation = getClass().getDeclaredAnnotation(annotationClass);
      if (annotation == null) return null;
      return modeRetriever.apply(annotation);
   }

   private MultipleCacheManagersTest internalCacheMode(CacheMode cacheMode) {
      this.parametrizedInstance = true;
      return cacheMode(cacheMode);
   }

   public MultipleCacheManagersTest cacheMode(CacheMode cacheMode) {
      this.cacheMode = cacheMode;
      return this;
   }

   public MultipleCacheManagersTest transactional(boolean transactional) {
      this.transactional = transactional;
      return this;
   }

   public MultipleCacheManagersTest lockingMode(LockingMode lockingMode) {
      this.lockingMode = lockingMode;
      return this;
   }

   public MultipleCacheManagersTest totalOrder(boolean totalOrder) {
      this.totalOrder = totalOrder;
      return this;
   }

   public MultipleCacheManagersTest isolationLevel(IsolationLevel isolationLevel) {
      this.isolationLevel = isolationLevel;
      return this;
   }

   public MultipleCacheManagersTest biasAcquisition(BiasAcquisition biasAcquisition) {
      this.biasAcquisition = biasAcquisition;
      return this;
   }

   public TransactionMode transactionMode() {
      return transactional ? TransactionMode.TRANSACTIONAL : TransactionMode.NON_TRANSACTIONAL;
   }

   @Override
   protected String parameters() {
      // cacheMode is self-explaining
      String[] names = parameterNames();
      Object[] params = parameterValues();
      assert names.length == params.length;

      boolean[] last = new boolean[params.length];
      boolean none = true;
      for (int i = params.length - 1; i >= 0; --i) {
         last[i] = none;
         none &= params[i] == null;
      }
      if (none) {
         return null;
      }
      StringBuilder sb = new StringBuilder().append('[');
      for (int i = 0; i < params.length; ++i) {
         if (params[i] != null) {
            if (names[i] != null) {
               sb.append(names[i]).append('=');
            }
            sb.append(params[i]);
            if (!last[i]) sb.append(", ");
         }
      }
      return sb.append(']').toString();
   }

   protected String[] parameterNames() {
      return new String[]{null, "tx", "locking", "TO", "isolation", "bias", "triangle"};
   }

   protected Object[] parameterValues() {
      return new Object[]{cacheMode, transactional, lockingMode, totalOrder, isolationLevel, biasAcquisition, useTriangle};
   }

   protected static <T> T[] concat(T[] a1, T... a2) {
      T[] na = Arrays.copyOf(a1, a1.length + a2.length);
      System.arraycopy(a2, 0, na, a1.length, a2.length);
      return na;
   }

   /**
    * Create the cache managers you need for your test.  Note that the cache managers you create *must* be created using
    * {@link #addClusterEnabledCacheManager()}
    */
   protected abstract void createCacheManagers() throws Throwable;

   protected Address address(int cacheIndex) {
      return manager(cacheIndex).getAddress();
   }

   protected <A, B> AdvancedCache<A, B> advancedCache(int i) {
      return this.<A, B>cache(i).getAdvancedCache();
   }

   protected <A, B> AdvancedCache<A, B> advancedCache(int i, String cacheName) {
      return this.<A, B>cache(i, cacheName).getAdvancedCache();
   }

   protected <K, V> List<Cache<K, V>> caches(String name) {
      return getCaches(name);
   }

   protected <K, V> List<Cache<K, V>> caches() {
      return caches(null);
   }

   protected Address address(Cache<?, ?> c) {
      return c.getAdvancedCache().getRpcManager().getAddress();
   }

   protected LockManager lockManager(int i) {
      return TestingUtil.extractLockManager(cache(i));
   }

   protected LockManager lockManager(int i, String cacheName) {
      return TestingUtil.extractLockManager(getCache(i, cacheName));
   }

   public List<EmbeddedCacheManager> getCacheManagers() {
      return cacheManagers;
   }

   /**
    * Kills the cache manager with the given index and waits for the new cluster to form.
    */
   protected void killMember(int cacheIndex) {
      killMember(cacheIndex, null);
   }

   /**
    * Kills the cache manager with the given index and waits for the new cluster to form using the provided cache
    */
   protected void killMember(int cacheIndex, String cacheName) {
      killMember(cacheIndex, cacheName, true);
   }

   protected void killMember(int cacheIndex, String cacheName, boolean awaitRehash) {
      List<Cache<Object, Object>> caches = caches(cacheName);
      caches.remove(cacheIndex);
      manager(cacheIndex).stop();
      cacheManagers.remove(cacheIndex);
      if (awaitRehash && caches.size() > 0) {
         TestingUtil.blockUntilViewsReceived(60000, false, caches);
         TestingUtil.waitForNoRebalance(caches);
      }
   }

   /**
    * Creates a {@link org.infinispan.affinity.KeyAffinityService} and uses it for generating a key that maps to the given address.
    * @param nodeIndex the index of tha cache where to be the main data owner of the returned key
    */
   protected Object getKeyForCache(int nodeIndex) {
      final Cache<Object, Object> cache = cache(nodeIndex);
      return getKeyForCache(cache);
   }

   protected Object getKeyForCache(int nodeIndex, String cacheName) {
      final Cache<Object, Object> cache = cache(nodeIndex, cacheName);
      return getKeyForCache(cache);
   }

   @SuppressWarnings("unchecked")
   protected <K> Supplier<K> supplyKeyForCache(int nodeIndex, String cacheName) {
      return () -> (K) getKeyForCache(nodeIndex, cacheName);
   }

   protected MagicKey getKeyForCache(Cache<?, ?> cache) {
      return new MagicKey(cache);
   }

   protected MagicKey getKeyForCache(Cache<?, ?> primary, Cache<?, ?>... backup) {
      if (cacheMode == null || !cacheMode.isScattered()) {
         return new MagicKey(primary, backup);
      } else {
         return new MagicKey(primary);
      }
   }

   protected void assertNotLocked(final String cacheName, final Object key) {
      eventually(() -> {
         boolean aNodeIsLocked = false;
         for (int i = 0; i < caches(cacheName).size(); i++) {
            final boolean isLocked = lockManager(i, cacheName).isLocked(key);
            if (isLocked)
               log.trace(key + " is locked on cache index " + i + " by " + lockManager(i, cacheName).getOwner(key));
            aNodeIsLocked = aNodeIsLocked || isLocked;
         }
         return !aNodeIsLocked;
      });
   }

   protected void assertNotLocked(final Object key) {
      assertNotLocked(null, key);
   }

   protected boolean checkTxCount(int cacheIndex, int localTx, int remoteTx) {
      final int localTxCount = TestingUtil.getTransactionTable(cache(cacheIndex)).getLocalTxCount();
      final int remoteTxCount = TestingUtil.getTransactionTable(cache(cacheIndex)).getRemoteTxCount();
      log.tracef("Cache index %s, local tx %4s, remote tx %4s \n", cacheIndex, localTxCount, remoteTxCount);
      return localTxCount == localTx && remoteTxCount == remoteTx;
   }

   protected void assertNotLocked(int cacheIndex, Object key) {
      assertEventuallyNotLocked(cache(cacheIndex), key);
   }

   protected void assertLocked(int cacheIndex, Object key) {
      assertLocked(cache(cacheIndex), key);
   }

   protected boolean checkLocked(int index, Object key) {
      return checkLocked(cache(index), key);
   }

   protected <K, V> Cache<K, V> getLockOwner(Object key) {
      return getLockOwner(key, null);
   }

   protected <K, V> Cache<K, V> getLockOwner(Object key, String cacheName) {
      Configuration c = getCache(0, cacheName).getCacheConfiguration();
      if (c.clustering().cacheMode().isInvalidation()) {
         return getCache(0, cacheName); //for replicated caches only the coordinator acquires lock
      } else if (!c.clustering().cacheMode().isClustered()) {
         throw new IllegalStateException("This is not a clustered cache!");
      } else {
         Address address = getCache(0, cacheName).getAdvancedCache().getDistributionManager().getCacheTopology()
               .getDistribution(key).primary();
         for (Cache<K, V> cache : this.<K, V>caches(cacheName)) {
            if (cache.getAdvancedCache().getRpcManager().getTransport().getAddress().equals(address)) {
               return cache;
            }
         }
         throw new IllegalStateException();
      }
   }

   protected void assertKeyLockedCorrectly(Object key) {
      assertKeyLockedCorrectly(key, null);
   }

   protected void assertKeyLockedCorrectly(Object key, String cacheName) {
      final Cache<?, ?> lockOwner = getLockOwner(key, cacheName);
      assert checkLocked(lockOwner, key);
      for (Cache<?, ?> c : caches(cacheName)) {
         if (c != lockOwner)
            assert !checkLocked(c, key) : "Key " + key + " is locked on cache " + c + " (" + cacheName
                  + ") and it shouldn't";
      }
   }

   private <K, V> Cache<K, V> getCache(int index, String name) {
      return name == null ? this.cache(index) : this.cache(index, name);
   }

   protected void forceTwoPhase(int cacheIndex) throws SystemException, RollbackException {
      TransactionManager tm = tm(cacheIndex);
      Transaction tx = tm.getTransaction();
      tx.enlistResource(new XAResourceAdapter());
   }

   protected void assertNoTransactions() {
      assertNoTransactions(null);
   }

   protected void assertNoTransactions(final String cacheName) {
      eventually("There are pending transactions!", () -> {
         for (Cache<?, ?> cache : caches(cacheName)) {
            final TransactionTable transactionTable = TestingUtil.extractComponent(cache, TransactionTable.class);
            int localTxCount = transactionTable.getLocalTxCount();
            int remoteTxCount = transactionTable.getRemoteTxCount();
            if (localTxCount != 0 || remoteTxCount != 0) {
               log.tracef("Local tx=%s, remote tx=%s, for cache %s ", transactionTable.getLocalGlobalTransaction(),
                     transactionTable.getRemoteGlobalTransaction(), address(cache));
               return false;
            }
         }
         return true;
      });
   }

   protected TransactionTable transactionTable(int cacheIndex) {
      return advancedCache(cacheIndex).getComponentRegistry()
            .getComponent(TransactionTable.class);
   }

   protected void assertEventuallyEquals(
         final int cacheIndex, final Object key, final Object value) {
      eventually(() -> value == null
            ? null == cache(cacheIndex).get(key)
            : value.equals(cache(cacheIndex).get(key)));
   }

   public MultipleCacheManagersTest useTriangle(boolean useTriangle) {
      this.useTriangle = useTriangle;
      return this;
   }

   protected abstract static class AnnotationFilter<A extends Annotation, AM, CM> {
      private final Class<A> annotationClazz;
      private final Function<A, AM[]> modesRetriever;
      private final BiPredicate<AM, CM> modeChecker;

      protected AnnotationFilter(Class<A> annotationClazz, Function<A, AM[]> modesRetriever, BiPredicate<AM, CM> modeChecker) {
         this.annotationClazz = annotationClazz;
         this.modesRetriever = modesRetriever;
         this.modeChecker = modeChecker;
      }

      public boolean test(CM mode, IMethodInstance method) {
         // If both method and class have the annotation, method annotation has priority.
         A clazzAnnotation = method.getInstance().getClass().getAnnotation(annotationClazz);
         A methodAnnotation = method.getMethod().getConstructorOrMethod().getMethod().getAnnotation(annotationClazz);
         if (methodAnnotation != null) {
            // If a method-level annotation contains current cache mode, run it, otherwise ignore that
            if (Stream.of(modesRetriever.apply(methodAnnotation)).anyMatch(m -> modeChecker.test(m, mode))) {
               return true;
            }
         } else if (clazzAnnotation != null) {
            return true;
         } else if (mode == null || !((MultipleCacheManagersTest) method.getInstance()).parametrizedInstance) {
            // There are no annotations on this method nor on this class, but due to an annotation
            // on different method there may be instances with non-default cache mode
            return true;
         }
         return false;
      }
   }

   public static class CacheModeFilter extends AnnotationFilter<InCacheMode, CacheMode, CacheMode> implements Predicate<IMethodInstance> {
      private final String cacheModeString = System.getProperty("test.infinispan.cacheMode");

      public CacheModeFilter() {
         super(InCacheMode.class, InCacheMode::value, (m1, m2) -> m1 == m2);
      }

      @Override
      public boolean test(IMethodInstance method) {
         CacheMode cacheMode = ((MultipleCacheManagersTest) method.getInstance()).cacheMode;
         if (cacheModeString != null && cacheMode != null && !cacheMode.friendlyCacheModeString().equalsIgnoreCase(cacheModeString)) {
            return false;
         }
         return test(cacheMode, method);
      }
   }

   public static class TransactionalModeFilter extends AnnotationFilter<InTransactionMode, TransactionMode, Boolean> implements Predicate<IMethodInstance> {
      private final String txModeString = System.getProperty("test.infinispan.transactional");

      public TransactionalModeFilter() {
         super(InTransactionMode.class, InTransactionMode::value, (m, b) -> b == Boolean.valueOf(m.isTransactional()));
      }

      @Override
      public boolean test(IMethodInstance method) {
         Boolean transactional = ((MultipleCacheManagersTest) method.getInstance()).transactional;
         if (txModeString != null && transactional != null && !transactional.toString().equalsIgnoreCase(txModeString)) {
            return false;
         }
         return test(transactional, method);
      }
   }

   protected static abstract class FilterByProperty<T> implements Predicate<IMethodInstance> {
      private final String property;
      // this could be done through abstract method but this way is more concise
      private final Function<MultipleCacheManagersTest, T> getMode;

      public FilterByProperty(String property, Function<MultipleCacheManagersTest, T> getMode) {
         this.property = System.getProperty(property);
         this.getMode = getMode;
      }

      @Override
      public boolean test(IMethodInstance method) {
         if (property == null) return true;
         T mode = getMode.apply((MultipleCacheManagersTest) method.getInstance());
         return mode == null || mode.toString().equalsIgnoreCase(property);
      }
   }

   public static class TotalOrderFilter extends FilterByProperty<Boolean> {
      public TotalOrderFilter() {
         super("test.infinispan.totalOrder", test -> test.totalOrder);
      }
   }

   public static class LockingModeFilter extends FilterByProperty<LockingMode> {
      public LockingModeFilter() {
         super("test.infinispan.lockingMode", test -> test.lockingMode);
      }
   }

   public static class IsolationLevelFilter extends FilterByProperty<IsolationLevel> {
      public IsolationLevelFilter() {
         super("test.infinispan.isolationLevel", test -> test.isolationLevel);
      }
   }

}
