package org.infinispan.configuration;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.Principal;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.configuration.JsonReader;
import org.infinispan.commons.configuration.JsonWriter;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.configuration.cache.PersistenceConfiguration;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.cache.XSiteStateTransferMode;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.conflict.MergePolicy;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.ch.impl.AffinityPartitioner;
import org.infinispan.distribution.ch.impl.TopologyAwareSyncConsistentHashFactory;
import org.infinispan.distribution.group.Grouper;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.filter.CacheFilters;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.globalstate.LocalConfigurationStorage;
import org.infinispan.interceptors.BaseAsyncInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.exts.MapExternalizer;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.PrincipalRoleMapperContext;
import org.infinispan.security.mappers.IdentityRoleMapper;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.data.Person;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(testName = "configuration.JsonSerializationTest", groups = "functional")
public class JsonSerializationTest extends AbstractInfinispanTest {

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   private final JsonReader jsonReader = new JsonReader();
   private final JsonWriter jsonWriter = new JsonWriter();

   @Test
   public void testMinimalCacheConfiguration() {
      Configuration minimal = new ConfigurationBuilder().build();
      testJsonConversion(minimal);
   }

   @Test
   public void testComplexCacheConfiguration() {
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      configurationBuilder
            .unsafe().unreliableReturnValues(true)
            .locking().isolationLevel(IsolationLevel.REPEATABLE_READ).lockAcquisitionTimeout(30, TimeUnit.MILLISECONDS).useLockStriping(true).concurrencyLevel(12)
            .clustering()
            .cacheMode(CacheMode.DIST_SYNC)
            .remoteTimeout(12, TimeUnit.SECONDS)

            .hash()
            .capacityFactor(23.4f)
            .consistentHashFactory(new TopologyAwareSyncConsistentHashFactory())
            .keyPartitioner(new AffinityPartitioner())
            .numOwners(2)
            .numSegments(123)

            .groups()
            .enabled(true)
            .addGrouper(new Grouper1()).addGrouper(new Grouper2())

            .l1().enable().lifespan(49).cleanupTaskFrequency(1201).invalidationThreshold(2)

            .stateTransfer().fetchInMemoryState(true).awaitInitialTransfer(false).timeout(13, TimeUnit.SECONDS).chunkSize(12)

            .partitionHandling().mergePolicy(MergePolicy.PREFERRED_ALWAYS).whenSplit(PartitionHandling.DENY_READ_WRITES)
            .statistics().enable()
            .transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .lockingMode(LockingMode.PESSIMISTIC).useSynchronization(false)
            .autoCommit(true).cacheStopTimeout(1, TimeUnit.HOURS)
            .reaperWakeUpInterval(1)
            .completedTxTimeout(123)
            .cacheStopTimeout(1, TimeUnit.SECONDS)
            .notifications(true)
            .recovery().enable().recoveryInfoCacheName("VTNC")

            .encoding().key().mediaType("application/x-java-object")
            .encoding().value().mediaType("application/x-java-object")

            .sites().addInUseBackupSite("CY").backupFor().remoteSite("QWERTY").remoteCache("cache")
            .sites().addBackup().site("NY").strategy(BackupConfiguration.BackupStrategy.ASYNC).backupFailurePolicy(BackupFailurePolicy.IGNORE)
            .sites().addBackup().site("CY").strategy(BackupConfiguration.BackupStrategy.ASYNC).backupFailurePolicy(BackupFailurePolicy.IGNORE)
            .failurePolicyClass("kkk").useTwoPhaseCommit(false).replicationTimeout(1231)
            .takeOffline()
            .afterFailures(1000)
            .minTimeToWait(42000)
            .backup().stateTransfer()
            .chunkSize(12)
            .timeout(1)
            .maxRetries(2)
            .waitTime(12)
            .mode(XSiteStateTransferMode.AUTO)

            .security().authorization().role("ROLE").role("ROLA")
            .persistence().passivation(false)
            .expiration().lifespan(10).wakeUpInterval(123).maxIdle(1122)
            .indexing().enable()
            .reader().refreshInterval(1000)
            .writer().ramBufferSize(100).maxBufferedEntries(123).queueSize(111).queueCount(12).setLowLevelTrace(true).commitInterval(12)
            .merge().factor(12).minSize(12).maxSize(11).calibrateByDeletes(true)
            .addIndexedEntity("Entity").addProperty("v", "v")
            .customInterceptors()
            .addInterceptor()
            .interceptorClass(AsyncInterceptor1.class)
            .position(InterceptorConfiguration.Position.OTHER_THAN_FIRST_OR_LAST)
            .customInterceptors()
            .addInterceptor()
            .interceptorClass(AsyncInterceptor2.class)
            .position(InterceptorConfiguration.Position.LAST)
            .invocationBatching().disable()
            .memory().size(123).storageType(StorageType.OBJECT).evictionStrategy(EvictionStrategy.REMOVE);

      Configuration configuration = configurationBuilder.build();

      testJsonConversion(configuration);
   }

   @Test
   public void testLatestVersion() throws IOException {
      Properties properties = new Properties();
      properties.put("jboss.server.temp.dir", System.getProperty("java.io.tmpdir"));
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader(), false, properties);
      ConfigurationBuilderHolder builderHolder = parserRegistry.parse(loadLatestVersionTest());
      testConfigurations(builderHolder);
   }

   private ConfigurationBuilderHolder parseStringConfiguration(String config) {
      InputStream is = new ByteArrayInputStream(config.getBytes());
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader(), true, System.getProperties());
      return parserRegistry.parse(is, null, MediaType.APPLICATION_XML);
   }

   @Test
   public void testResourcesConfiguration() throws IOException {
      ParserRegistry parserRegistry = new ParserRegistry();
      File[] resourceConfigs = getResourceConfigs();
      for (File f : resourceConfigs) {
         if (f.isDirectory()) continue;
         ConfigurationBuilderHolder builderHolder = parserRegistry.parse(f.toURI().toURL());
         testConfigurations(builderHolder);
      }
   }

   @Test
   public void testDummyInMemoryStore() {
      String config = TestingUtil.wrapXMLWithoutSchema(
            "<cache-container default-cache=\"default\"><local-cache name=\"default\">\n" +
                  "<persistence ><store class=\"org.infinispan.persistence.dummy.DummyInMemoryStore\" >\n" +
                  "<property name=\"storeName\">myStore</property>" +
                  "</store></persistence></local-cache></cache-container>"
      );
      ConfigurationBuilderHolder holder = parseStringConfiguration(config);
      PersistenceConfiguration cfg = holder.getDefaultConfigurationBuilder().build().persistence();
      ConfigurationBuilder before = holder.getNamedConfigurationBuilders().get("default");

      String toJSON = jsonWriter.toJSON(cfg);
      ConfigurationBuilder after = new ConfigurationBuilder();
      jsonReader.readJson(after, toJSON);

      assertEquals(before.build().persistence(), after.persistence().build().persistence());
   }

   @Test
   public void testMinimalCacheManager() {
      GlobalConfiguration globalConfiguration = new GlobalConfigurationBuilder().build();

      String json = jsonWriter.toJSON(globalConfiguration);
      Json jsonNode = Json.read(json);

      Json cacheContainer = jsonNode.at("infinispan").at("cache-container");
      assertEquals("DefaultCacheManager", cacheContainer.at("name").asString());
   }

   @Test
   public void testClusteredCacheManager() {
      GlobalConfiguration globalConfiguration = new GlobalConfigurationBuilder().clusteredDefault().build();


      String json = jsonWriter.toJSON(globalConfiguration);
      Json jsonNode = Json.read(json);

      Json cacheContainer = jsonNode.at("infinispan").at("cache-container");
      assertEquals("DefaultCacheManager", cacheContainer.at("name").asString());

      Json jgroups = jsonNode.at("infinispan").at("jgroups");
      assertEquals("org.infinispan.remoting.transport.jgroups.JGroupsTransport", jgroups.at("transport").asString());
   }

   @Test
   public void testGlobalAuthorization() {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder().clusteredDefault();
      builder.security().authorization().principalRoleMapper(new DummyRoleMapper()).create();

      String toJSON = jsonWriter.toJSON(builder.build());

      Json node = Json.read(toJSON);
      Json authz = node.at("infinispan").at("cache-container").at("security").at("authorization");

      assertEquals(DummyRoleMapper.class.getName(), authz.at("custom-role-mapper").at("class").asString());
   }

   @Test
   public void testFullConfig() throws Exception {
      ParserRegistry parserRegistry = new ParserRegistry();

      ConfigurationBuilderHolder holder = parserRegistry.parseFile("configs/named-cache-test.xml");

      GlobalConfiguration configuration = holder.getGlobalConfigurationBuilder().build();

      String toJSON = jsonWriter.toJSON(configuration);
      Json node = Json.read(toJSON);

      Json infinispan = node.at("infinispan");
      Json threads = infinispan.at("threads");
      Json cacheContainer = infinispan.at("cache-container");
      Json globalSecurity = cacheContainer.at("security");

      Json threadFactories = threads.at("thread-factory");
      assertEquals(5, threadFactories.asList().size());
      Iterator<Json> elements = threadFactories.asJsonList().iterator();
      assertThreadFactory(elements.next(), "listener-factory", "infinispan", "AsyncListenerThread", "1");
      assertThreadFactory(elements.next(), "blocking-factory", "infinispan", "BlockingThread", "1");
      assertThreadFactory(elements.next(), "non-blocking-factory", "infinispan", "NonBlockingThread", "1");
      assertThreadFactory(elements.next(), "expiration-factory", "infinispan", "ExpirationThread", "1");
      assertThreadFactory(elements.next(), "replication-queue-factory", "infinispan", "ReplicationQueueThread", "1");

      Json boundedThreadPools = threads.at("blocking-bounded-queue-thread-pool");
      elements = boundedThreadPools.asJsonList().iterator();
      assertEquals(3, boundedThreadPools.asList().size());
      assertBoundedThreadPool(elements.next(), "listener", "listener-factory", "5", "0", "10000", "0");
      assertBoundedThreadPool(elements.next(), "blocking", "blocking-factory", "6", "0", "10001", "0");
      assertBoundedThreadPool(elements.next(), "non-blocking", "non-blocking-factory", "5", "5", "10000", "0");

      Json scheduledThreadPools = threads.at("scheduled-thread-pool");
      elements = scheduledThreadPools.asJsonList().iterator();
      assertEquals(2, scheduledThreadPools.asList().size());
      assertScheduledThreadPool(elements.next(), "expiration", "expiration-factory");
      assertScheduledThreadPool(elements.next(), "replication-queue", "replication-queue-factory");

      assertEquals("DefaultCacheManager", cacheContainer.at("name").asString());
      assertEquals("default", cacheContainer.at("default-cache").asString());
      assertEquals("REGISTER", cacheContainer.at("shutdown-hook").asString());
      assertTrue(cacheContainer.at("statistics").asBoolean());
      assertEquals("listener", cacheContainer.at("listener-executor").asString());
      assertNull(cacheContainer.at("async-executor"));
      assertEquals("non-blocking", cacheContainer.at("non-blocking-executor").asString());
      assertEquals("blocking", cacheContainer.at("blocking-executor").asString());

      Json authorization = globalSecurity.at("authorization");
      assertEquals("org.infinispan.security.audit.NullAuditLogger", authorization.at("audit-logger").asString());
      Json roleMapper = authorization.at("identity-role-mapper");
      assertNotNull(roleMapper);
      assertEquals(0, roleMapper.asMap().size());
      Json roles = authorization.at("roles");
      assertRole(roles, "vavasour", "READ", "WRITE");
      assertRole(roles, "peasant", "READ");
      assertRole(roles, "king", "ALL");
      assertRole(roles, "vassal", "READ", "WRITE", "LISTEN");
   }

   @Test
   public void testJGroups() throws IOException {
      ParserRegistry parserRegistry = new ParserRegistry();
      ConfigurationBuilderHolder holder = parserRegistry.parseFile("configs/config-with-jgroups-stack.xml");
      GlobalConfiguration configuration = holder.getGlobalConfigurationBuilder().build();

      String toJSON = jsonWriter.toJSON(configuration);
      Json node = Json.read(toJSON);

      Json infinispan = node.at("infinispan");
      Json jgroups = infinispan.at("jgroups");

      assertEquals(JGroupsTransport.class.getName(), jgroups.at("transport").asString());

      Json stackFile = jgroups.at("stack-file");
      assertEquals(8, stackFile.asList().size());
      Iterator<Json> stackFiles = stackFile.asJsonList().iterator();
      assertStackFile(stackFiles.next(), "tcp", "default-configs/default-jgroups-tcp.xml");
      assertStackFile(stackFiles.next(), "udp", "default-configs/default-jgroups-udp.xml");
      assertStackFile(stackFiles.next(), "kubernetes", "default-configs/default-jgroups-kubernetes.xml");
      assertStackFile(stackFiles.next(), "ec2", "default-configs/default-jgroups-ec2.xml");
      assertStackFile(stackFiles.next(), "google", "default-configs/default-jgroups-google.xml");
      assertStackFile(stackFiles.next(), "azure", "default-configs/default-jgroups-azure.xml");
      assertStackFile(stackFiles.next(), "udp-test", "stacks/udp.xml");
      assertStackFile(stackFiles.next(), "tcp-test", "stacks/tcp_mping/tcp1.xml");

      Json stack = jgroups.at("stack");
      assertEquals(5, stack.asList().size());

      Json mping = stack.asJsonList().iterator().next();
      assertEquals(14, mping.asMap().size());
      assertEquals("mping", mping.at("name").asString());

      Json tcp = mping.at("TCP");
      assertEquals(11, tcp.asMap().size());
      assertEquals("127.0.0.1", tcp.at("bind_addr").asString());
      assertEquals("7800", tcp.at("bind_port").asString());
      assertEquals("30", tcp.at("port_range").asString());
      assertEquals("20000000", tcp.at("recv_buf_size").asString());
      assertEquals("640000", tcp.at("send_buf_size").asString());
      assertEquals("300", tcp.at("sock_conn_timeout").asString());
      assertEquals("transfer-queue", tcp.at("bundler_type").asString());
      assertEquals("0", tcp.at("thread_pool.min_threads").asString());
      assertEquals("25", tcp.at("thread_pool.max_threads").asString());
      assertEquals("5000", tcp.at("thread_pool.keep_alive_time").asString());
      assertEquals("10000", tcp.at("thread_dumps_threshold").asString());

      Json fdSock = mping.at("FD_SOCK");
      assertEquals(0, fdSock.asMap().size());
   }

   private void assertRole(Json role, String roleName, String... roles) {
      Json roleNode = role.at(roleName);
      Set<String> expected = Arrays.stream(roles).collect(Collectors.toSet());
      Set<String> nodeRoles = asSet(roleNode);
      assertEquals(expected, nodeRoles);
   }

   private void assertStackFile(Json node, String name, String path) {
      assertEquals(node.at("name").asString(), name);
      assertEquals(node.at("path").asString(), path);
   }

   private void assertThreadFactory(Json node, String name, String groupName, String pattern, String prio) {
      assertEquals(node.at("name").asString(), name);
      assertEquals(node.at("group-name").asString(), groupName);
      assertEquals(node.at("thread-name-pattern").asString(), pattern);
      assertEquals(node.at("priority").asString(), prio);
   }

   private void assertBoundedThreadPool(Json node, String name, String threadFactory, String maxThreads,
                                        String coreThreads, String queueLength, String keepAliveTime) {
      assertEquals(node.at("name").asString(), name);
      assertEquals(node.at("thread-factory").asString(), threadFactory);
      assertEquals(node.at("max-threads").asString(), maxThreads);
      assertEquals(node.at("core-threads").asString(), coreThreads);
      assertEquals(node.at("queue-length").asString(), queueLength);
      assertEquals(node.at("keep-alive-time").asString(), keepAliveTime);
   }

   private void assertScheduledThreadPool(Json node, String name, String threadFactory) {
      assertEquals(node.at("name").asString(), name);
      assertEquals(node.at("thread-factory").asString(), threadFactory);
   }

   @Test
   public void testWithDefaultCache() {
      GlobalConfiguration globalConfiguration = new GlobalConfigurationBuilder()
            .cacheManagerName("my-cm").defaultCacheName("default-one").build();
      String json = jsonWriter.toJSON(globalConfiguration);

      Json jsonNode = Json.read(json);
      Json cacheContainer = jsonNode.at("infinispan").at("cache-container");
      assertEquals("my-cm", cacheContainer.at("name").asString());
      assertEquals("default-one", cacheContainer.at("default-cache").asString());
   }

   @Test
   public void testWithSecurity() {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
      builder.security().authorization().enabled(true).principalRoleMapper(new IdentityRoleMapper())
            .role("role1").permission("ADMIN").permission("READ")
            .role("role2").permission(AuthorizationPermission.WRITE).create();

      GlobalConfiguration configuration = builder.build();
      String actual = jsonWriter.toJSON(configuration);
      Json jsonNode = Json.read(actual);

      Json cacheContainer = jsonNode.at("infinispan").at("cache-container");
      assertEquals("DefaultCacheManager", cacheContainer.at("name").asString());

      Json roles = cacheContainer.at("security").at("authorization").at("roles");
      assertEquals(2, roles.asMap().size());

      Json role1 = roles.at("role1");
      assertEquals(2, role1.asList().size());
      assertEquals(newSet("ADMIN", "READ"), asSet(role1));

      Json role2 = roles.at("role2");
      assertEquals(1, role2.asList().size());
      assertEquals(newSet("WRITE"), asSet(role2));
   }

   @Test
   public void testGlobalState() {
      GlobalConfiguration globalConfiguration = new GlobalConfigurationBuilder().globalState().enable()
            .persistentLocation("/tmp/location")
            .sharedPersistentLocation("/tmp/shared")
            .temporaryLocation("/tmp/temp")
            .configurationStorage(ConfigurationStorage.VOLATILE).build();

      String json = jsonWriter.toJSON(globalConfiguration);

      Json jsonNode = Json.read(json);

      Json cacheContainer = jsonNode.at("infinispan").at("cache-container");
      assertEquals("DefaultCacheManager", cacheContainer.at("name").asString());
      Json globalState = cacheContainer.at("global-state");
      assertEquals("/tmp/location", globalState.at("persistent-location").at("path").asString());
      assertEquals("/tmp/shared", globalState.at("shared-persistent-location").at("path").asString());
      assertEquals("/tmp/temp", globalState.at("temporary-location").at("path").asString());
      Json storageConfig = globalState.at("volatile-configuration-storage");
      assertNotNull(storageConfig);
      assertEquals(0, storageConfig.asMap().size());

      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
      builder.globalState().enable()
            .temporaryLocation("/tmp/temp")
            .configurationStorage(ConfigurationStorage.CUSTOM)
            .configurationStorageSupplier(TestStorage::new).create();

      json = jsonWriter.toJSON(builder.build());
      jsonNode = Json.read(json);

      Json cfgStorage = jsonNode.at("infinispan").at("cache-container").at("global-state").at("custom-configuration-storage");
      assertNotNull(cfgStorage);
      assertNotNull(TestStorage.class.getName(), cfgStorage.at("class"));
   }

   @Test
   public void testSerializationConfig() {
      final String regexp = "org.infinispan.test.*";
      AdvancedExternalizer<?> mapExternalizer = new MapExternalizer();
      AdvancedExternalizer<?> opExternalizer = new CacheFilters.CacheFiltersExternalizer();
      GlobalConfigurationBuilder globalConfigurationBuilder = new GlobalConfigurationBuilder();
      globalConfigurationBuilder.serialization()
            .marshaller(new JavaSerializationMarshaller())
            .addAdvancedExternalizer(1, mapExternalizer)
            .addAdvancedExternalizer(2, opExternalizer)
            .allowList()
            .addClass(Person.class.getName())
            .addRegexp(regexp).create();

      GlobalConfiguration globalConfiguration = globalConfigurationBuilder.build();
      String json = jsonWriter.toJSON(globalConfiguration);

      Json node = Json.read(json);
      Json serialization = node.at("infinispan").at("cache-container").at("serialization");
      assertEquals(JavaSerializationMarshaller.class.getName(), serialization.at("marshaller").asString());
      Json externalizerMap = serialization.at("advanced-externalizer");
      assertEquals(MapExternalizer.class.getName(), externalizerMap.at("1").asString());
      assertEquals(CacheFilters.CacheFiltersExternalizer.class.getName(), externalizerMap.at("2").asString());

      Json allowList = serialization.at("allow-list");
      Json classes = allowList.at("classes");
      assertTrue(classes.isArray());
      assertEquals(Person.class.getName(), classes.asJsonList().iterator().next().asString());

      Json regexps = allowList.at("regexps");
      assertTrue(regexps.isArray());
      assertEquals(regexp, regexps.asJsonList().iterator().next().asString());
   }

   @Test
   public void testJmx() {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
      builder.cacheContainer().statistics(true)
            .jmx().enabled(true)
            .domain("x")
            .mBeanServerLookup(mBeanServerLookup)
            .addProperty("prop1", "val1")
            .addProperty("prop2", "val2");

      GlobalConfiguration configuration = builder.build();

      String toJSON = jsonWriter.toJSON(configuration);

      Json node = Json.read(toJSON);
      Json cacheContainer = node.at("infinispan").at("cache-container");
      Json jmx = cacheContainer.at("jmx");

      assertTrue(cacheContainer.at("statistics").asBoolean());
      assertTrue(jmx.at("enabled").asBoolean());
      assertEquals("x", jmx.at("domain").asString());
      assertEquals(TestMBeanServerLookup.class.getName(), jmx.at("mbean-server-lookup").asString());
      assertEquals("val1", jmx.at("properties").at("prop1").asString());
      assertEquals("val2", jmx.at("properties").at("prop2").asString());
   }

   private Set<String> asSet(Json node) {
      return node.asJsonList().stream().map(Json::asString).collect(Collectors.toSet());
   }

   private Set<String> newSet(String... values) {
      return Arrays.stream(values).collect(Collectors.toSet());
   }

   private void testConfigurations(ConfigurationBuilderHolder builderHolder) {
      builderHolder.getNamedConfigurationBuilders().forEach((key, builder) -> {
         Configuration configuration = builder.build();
         testJsonConversion(key, configuration);
      });
   }

   private void testJsonConversion(String key, Configuration configuration) {
      String json = jsonWriter.toJSON(configuration);
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      jsonReader.readJson(configurationBuilder, json);
      Configuration fromJson = configurationBuilder.build();

      assertEquals(String.format("JSON conversion failed for configuration named '%s'", key), configuration.toXMLString(), fromJson.toXMLString());
   }

   private void testJsonConversion(Configuration configuration) {
      testJsonConversion("", configuration);
   }

   private File[] getResourceConfigs() {
      ClassLoader loader = this.getClass().getClassLoader();
      URL url = loader.getResource("configs");
      if (url == null) Assert.fail("Unable to find configurations!");
      return new File(url.getPath()).listFiles();
   }

   private URL loadLatestVersionTest() {
      String majorMinor = Version.getMajorMinor();
      ClassLoader loader = this.getClass().getClassLoader();
      String testFile = "configs/unified/" + majorMinor + ".xml";
      URL resource = loader.getResource(testFile);
      if (resource == null) {
         Assert.fail(String.format("Unable to find test configuration file '%s'", testFile));
      }
      return resource;
   }

   static class AsyncInterceptor1 extends BaseAsyncInterceptor {
      @Override
      public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
         return null;
      }
   }

   static class AsyncInterceptor2 extends AsyncInterceptor1 {
   }

   public static class Grouper1 implements Grouper<String> {

      @Override
      public Class<String> getKeyType() {
         return null;
      }
   }

   public static class Grouper2 implements Grouper<String> {

      @Override
      public Class<String> getKeyType() {
         return null;
      }
   }

   public static class DummyRoleMapper implements PrincipalRoleMapper {

      @Override
      public Set<String> principalToRoles(Principal principal) {
         return null;
      }

      @Override
      public void setContext(PrincipalRoleMapperContext context) {

      }
   }

   public static class TestStorage implements LocalConfigurationStorage {

      @Override
      public void initialize(EmbeddedCacheManager embeddedCacheManager, ConfigurationManager configurationManager, BlockingManager blockingManager) {
      }

      @Override
      public void validateFlags(EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      }

      @Override
      public CompletableFuture<Void> createCache(String name, String template, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
         return CompletableFutures.completedNull();
      }

      @Override
      public CompletableFuture<Void> removeCache(String name, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
         return CompletableFutures.completedNull();
      }

      @Override
      public CompletableFuture<Void> createTemplate(String name, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
         return CompletableFutures.completedNull();
      }

      @Override
      public CompletableFuture<Void> removeTemplate(String name, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
         return CompletableFutures.completedNull();
      }

      @Override
      public Map<String, Configuration> loadAllCaches() {
         return null;
      }

      @Override
      public Map<String, Configuration> loadAllTemplates() {
         return null;
      }
   }
}
