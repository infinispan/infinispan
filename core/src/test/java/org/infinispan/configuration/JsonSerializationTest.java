package org.infinispan.configuration;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
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
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.configuration.JsonReader;
import org.infinispan.commons.configuration.JsonWriter;
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
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.configuration.parsing.URLXMLResourceResolver;
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
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Test(testName = "configuration.JsonSerializationTest", groups = "functional")
public class JsonSerializationTest extends AbstractInfinispanTest {

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   private JsonReader jsonReader = new JsonReader();
   private JsonWriter jsonWriter = new JsonWriter();

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
            .remoteTimeout(12, TimeUnit.DAYS)

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

            .stateTransfer().fetchInMemoryState(true).awaitInitialTransfer(false).timeout(13).chunkSize(12)

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

            .encoding().key().mediaType("application/json")
            .encoding().value().mediaType("text/plain")

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


            .security().authorization().role("ROLE").role("ROLA")
            .persistence().passivation(false)
            .expiration().lifespan(10).wakeUpInterval(123).maxIdle(1122)
            .indexing().autoConfig(true).addProperty("v", "v")
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
      return parserRegistry.parse(is, null);
   }

   @Test
   public void testResourcesConfiguration() throws IOException {
      ParserRegistry parserRegistry = new ParserRegistry();
      File[] resourceConfigs = getResourceConfigs();
      for (File f : resourceConfigs) {
         if (f.isDirectory()) continue;
         ConfigurationBuilderHolder builderHolder = parserRegistry.parse(new FileInputStream(f), new URLXMLResourceResolver(f.toURI().toURL()));
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
   public void testMinimalCacheManager() throws IOException {
      GlobalConfiguration globalConfiguration = new GlobalConfigurationBuilder().build();

      String json = jsonWriter.toJSON(globalConfiguration);
      JsonNode jsonNode = new ObjectMapper().readTree(json);

      JsonNode cacheContainer = jsonNode.get("infinispan").get("cache-container");
      assertEquals("DefaultCacheManager", cacheContainer.get("name").asText());
   }

   @Test
   public void testClusteredCacheManager() throws IOException {
      GlobalConfiguration globalConfiguration = new GlobalConfigurationBuilder().clusteredDefault().build();

      String json = jsonWriter.toJSON(globalConfiguration);
      JsonNode jsonNode = new ObjectMapper().readTree(json);

      JsonNode cacheContainer = jsonNode.get("infinispan").get("cache-container");
      assertEquals("DefaultCacheManager", cacheContainer.get("name").asText());

      JsonNode jgroups = jsonNode.get("infinispan").get("jgroups");
      assertEquals("org.infinispan.remoting.transport.jgroups.JGroupsTransport", jgroups.get("transport").asText());
   }

   @Test
   public void testGlobalAuthorization() throws IOException {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder().clusteredDefault();
      builder.security().authorization().principalRoleMapper(new DummyRoleMapper()).create();

      String toJSON = jsonWriter.toJSON(builder.build());

      JsonNode node = new ObjectMapper().readTree(toJSON);
      JsonNode authz = node.get("infinispan").get("cache-container").get("security").get("authorization");

      assertEquals(DummyRoleMapper.class.getName(), authz.get("custom-role-mapper").get("class").asText());
   }

   @Test
   public void testFullConfig() throws Exception {
      ParserRegistry parserRegistry = new ParserRegistry();

      ConfigurationBuilderHolder holder = parserRegistry.parseFile("configs/named-cache-test.xml");

      GlobalConfiguration configuration = holder.getGlobalConfigurationBuilder().build();

      String toJSON = jsonWriter.toJSON(configuration);
      JsonNode node = new ObjectMapper().readTree(toJSON);

      JsonNode infinispan = node.get("infinispan");
      JsonNode threads = infinispan.get("threads");
      JsonNode cacheContainer = infinispan.get("cache-container");
      JsonNode globalSecurity = cacheContainer.get("security");

      ArrayNode threadFactories = (ArrayNode) threads.get("thread-factory");
      assertEquals(5, threadFactories.size());
      Iterator<JsonNode> elements = threadFactories.elements();
      assertThreadFactory(elements.next(), "listener-factory", "infinispan", "AsyncListenerThread", "1");
      assertThreadFactory(elements.next(), "blocking-factory", "infinispan", "BlockingThread", "1");
      assertThreadFactory(elements.next(), "non-blocking-factory", "infinispan", "NonBlockingThread", "1");
      assertThreadFactory(elements.next(), "expiration-factory", "infinispan", "ExpirationThread", "1");
      assertThreadFactory(elements.next(), "replication-queue-factory", "infinispan", "ReplicationQueueThread", "1");

      ArrayNode boundedThreadPools = (ArrayNode) threads.get("blocking-bounded-queue-thread-pool");
      elements = boundedThreadPools.elements();
      assertEquals(3, boundedThreadPools.size());
      assertBoundedThreadPool(elements.next(), "listener", "listener-factory", "5", "0", "10000", "0");
      assertBoundedThreadPool(elements.next(), "blocking", "blocking-factory", "6", "0", "10001", "0");
      assertBoundedThreadPool(elements.next(), "non-blocking", "non-blocking-factory", "5", "5", "10000", "0");

      ArrayNode scheduledThreadPools = (ArrayNode) threads.get("scheduled-thread-pool");
      elements = scheduledThreadPools.elements();
      assertEquals(2, scheduledThreadPools.size());
      assertScheduledThreadPool(elements.next(), "expiration", "expiration-factory");
      assertScheduledThreadPool(elements.next(), "replication-queue", "replication-queue-factory");

      assertEquals("DefaultCacheManager", cacheContainer.get("name").asText());
      assertEquals("default", cacheContainer.get("default-cache").asText());
      assertEquals("REGISTER", cacheContainer.get("shutdown-hook").asText());
      assertTrue(cacheContainer.get("statistics").asBoolean());
      assertEquals("listener", cacheContainer.get("listener-executor").asText());
      assertNull(cacheContainer.get("async-executor"));
      assertEquals("non-blocking", cacheContainer.get("non-blocking-executor").asText());
      assertEquals("blocking", cacheContainer.get("blocking-executor").asText());

      JsonNode authorization = globalSecurity.get("authorization");
      assertEquals("org.infinispan.security.audit.NullAuditLogger", authorization.get("audit-logger").asText());
      JsonNode roleMapper = authorization.get("identity-role-mapper");
      assertNotNull(roleMapper);
      assertEquals(0, roleMapper.size());
      JsonNode roles = authorization.get("roles");
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
      JsonNode node = new ObjectMapper().readTree(toJSON);

      JsonNode infinispan = node.get("infinispan");
      JsonNode jgroups = infinispan.get("jgroups");

      assertEquals(JGroupsTransport.class.getName(), jgroups.get("transport").asText());

      JsonNode stackFile = jgroups.get("stack-file");
      assertEquals(8, stackFile.size());
      Iterator<JsonNode> stackFiles = stackFile.elements();
      assertStackFile(stackFiles.next(), "tcp", "default-configs/default-jgroups-tcp.xml");
      assertStackFile(stackFiles.next(), "udp", "default-configs/default-jgroups-udp.xml");
      assertStackFile(stackFiles.next(), "kubernetes", "default-configs/default-jgroups-kubernetes.xml");
      assertStackFile(stackFiles.next(), "ec2", "default-configs/default-jgroups-ec2.xml");
      assertStackFile(stackFiles.next(), "google", "default-configs/default-jgroups-google.xml");
      assertStackFile(stackFiles.next(), "azure", "default-configs/default-jgroups-azure.xml");
      assertStackFile(stackFiles.next(), "udp-test", "stacks/udp.xml");
      assertStackFile(stackFiles.next(), "tcp-test", "stacks/tcp_mping/tcp1.xml");

      JsonNode stack = jgroups.get("stack");
      assertEquals(5, stack.size());

      JsonNode mping = stack.elements().next();
      assertEquals(14, mping.size());
      assertEquals("mping", mping.get("name").asText());

      ObjectNode tcp = (ObjectNode) mping.get("TCP");
      assertEquals(9, tcp.size());
      assertEquals("7800", tcp.get("bind_port").asText());
      assertEquals("30", tcp.get("port_range").asText());
      assertEquals("20000000", tcp.get("recv_buf_size").asText());
      assertEquals("640000", tcp.get("send_buf_size").asText());
      assertEquals("300", tcp.get("sock_conn_timeout").asText());
      assertEquals("no-bundler", tcp.get("bundler_type").asText());
      assertEquals("0", tcp.get("thread_pool.min_threads").asText());
      assertEquals("25", tcp.get("thread_pool.max_threads").asText());
      assertEquals("5000", tcp.get("thread_pool.keep_alive_time").asText());

      ObjectNode fdSock = (ObjectNode) mping.get("FD_SOCK");
      assertEquals(0, fdSock.size());
   }

   private void assertRole(JsonNode role, String roleName, String... roles) {
      ArrayNode roleNode = (ArrayNode) role.get(roleName);
      Set<String> expected = Arrays.stream(roles).collect(Collectors.toSet());
      Set<String> nodeRoles = asSet(roleNode);
      assertEquals(expected, nodeRoles);
   }

   private void assertStackFile(JsonNode node, String name, String path) {
      assertEquals(node.get("name").asText(), name);
      assertEquals(node.get("path").asText(), path);
   }

   private void assertThreadFactory(JsonNode node, String name, String groupName, String pattern, String prio) {
      assertEquals(node.get("name").asText(), name);
      assertEquals(node.get("group-name").asText(), groupName);
      assertEquals(node.get("thread-name-pattern").asText(), pattern);
      assertEquals(node.get("priority").asText(), prio);
   }

   private void assertBoundedThreadPool(JsonNode node, String name, String threadFactory, String maxThreads,
                                        String coreThreads, String queueLength, String keepAliveTime) {
      assertEquals(node.get("name").asText(), name);
      assertEquals(node.get("thread-factory").asText(), threadFactory);
      assertEquals(node.get("max-threads").asText(), maxThreads);
      assertEquals(node.get("core-threads").asText(), coreThreads);
      assertEquals(node.get("queue-length").asText(), queueLength);
      assertEquals(node.get("keep-alive-time").asText(), keepAliveTime);
   }

   private void assertScheduledThreadPool(JsonNode node, String name, String threadFactory) {
      assertEquals(node.get("name").asText(), name);
      assertEquals(node.get("thread-factory").asText(), threadFactory);
   }

   @Test
   public void testWithDefaultCache() throws IOException {
      GlobalConfiguration globalConfiguration = new GlobalConfigurationBuilder()
            .cacheManagerName("my-cm").defaultCacheName("default-one").build();
      String json = jsonWriter.toJSON(globalConfiguration);

      JsonNode jsonNode = new ObjectMapper().readTree(json);
      JsonNode cacheContainer = jsonNode.get("infinispan").get("cache-container");
      assertEquals("my-cm", cacheContainer.get("name").asText());
      assertEquals("default-one", cacheContainer.get("default-cache").asText());
   }

   @Test
   public void testWithSecurity() throws IOException {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
      builder.security().authorization().enabled(true).principalRoleMapper(new IdentityRoleMapper())
            .role("role1").permission("ADMIN").permission("READ")
            .role("role2").permission(AuthorizationPermission.WRITE).create();

      GlobalConfiguration configuration = builder.build();
      String actual = jsonWriter.toJSON(configuration);
      JsonNode jsonNode = new ObjectMapper().readTree(actual);

      JsonNode cacheContainer = jsonNode.get("infinispan").get("cache-container");
      assertEquals("DefaultCacheManager", cacheContainer.get("name").asText());

      JsonNode roles = cacheContainer.get("security").get("authorization").get("roles");
      assertEquals(2, roles.size());

      ArrayNode role1 = (ArrayNode) roles.get("role1");
      assertEquals(2, role1.size());
      assertEquals(newSet("ADMIN", "READ"), asSet(role1));

      ArrayNode role2 = (ArrayNode) roles.get("role2");
      assertEquals(1, role2.size());
      assertEquals(newSet("WRITE"), asSet(role2));
   }

   @Test
   public void testGlobalState() throws IOException {
      GlobalConfiguration globalConfiguration = new GlobalConfigurationBuilder().globalState().enable()
            .persistentLocation("/tmp/location")
            .sharedPersistentLocation("/tmp/shared")
            .temporaryLocation("/tmp/temp")
            .configurationStorage(ConfigurationStorage.VOLATILE).build();

      String json = jsonWriter.toJSON(globalConfiguration);

      JsonNode jsonNode = new ObjectMapper().readTree(json);

      JsonNode cacheContainer = jsonNode.get("infinispan").get("cache-container");
      assertEquals("DefaultCacheManager", cacheContainer.get("name").asText());
      JsonNode globalState = cacheContainer.get("global-state");
      assertEquals("/tmp/location", globalState.get("persistent-location").get("path").asText());
      assertEquals("/tmp/shared", globalState.get("shared-persistent-location").get("path").asText());
      assertEquals("/tmp/temp", globalState.get("temporary-location").get("path").asText());
      JsonNode storageConfig = globalState.get("volatile-configuration-storage");
      assertNotNull(storageConfig);
      assertEquals(0, storageConfig.size());

      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
      builder.globalState().enable()
            .temporaryLocation("/tmp/temp")
            .configurationStorage(ConfigurationStorage.CUSTOM)
            .configurationStorageSupplier(TestStorage::new).create();

      json = jsonWriter.toJSON(builder.build());
      System.out.println(json);
      jsonNode = new ObjectMapper().readTree(json);

      JsonNode cfgStorage = jsonNode.get("infinispan").get("cache-container").get("global-state").get("custom-configuration-storage");
      assertNotNull(cfgStorage);
      assertNotNull(TestStorage.class.getName(), cfgStorage.get("class"));
   }

   @Test
   public void testSerializationConfig() throws IOException {
      final String regexp = "org.infinispan.test.*";
      AdvancedExternalizer<?> mapExternalizer = new MapExternalizer();
      AdvancedExternalizer<?> opExternalizer = new CacheFilters.CacheFiltersExternalizer();
      GlobalConfigurationBuilder globalConfigurationBuilder = new GlobalConfigurationBuilder();
      globalConfigurationBuilder.serialization()
            .marshaller(new JavaSerializationMarshaller())
            .addAdvancedExternalizer(1, mapExternalizer)
            .addAdvancedExternalizer(2, opExternalizer)
            .whiteList()
            .addClass(Person.class.getName())
            .addRegexp(regexp).create();

      GlobalConfiguration globalConfiguration = globalConfigurationBuilder.build();
      String json = jsonWriter.toJSON(globalConfiguration);

      JsonNode node = new ObjectMapper().readTree(json);
      JsonNode serialization = node.get("infinispan").get("cache-container").get("serialization");
      assertEquals(JavaSerializationMarshaller.class.getName(), serialization.get("marshaller").asText());
      JsonNode externalizerMap = serialization.get("advanced-externalizer");
      assertEquals(MapExternalizer.class.getName(), externalizerMap.get("1").asText());
      assertEquals(CacheFilters.CacheFiltersExternalizer.class.getName(), externalizerMap.get("2").asText());

      JsonNode whiteList = serialization.get("white-list");
      JsonNode classes = whiteList.get("classes");
      assertTrue(classes.isArray());
      assertEquals(Person.class.getName(), classes.iterator().next().asText());

      JsonNode regexps = whiteList.get("regexps");
      assertTrue(regexps.isArray());
      assertEquals(regexp, regexps.iterator().next().asText());
   }

   @Test
   public void testJmx() throws IOException {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
      builder.cacheContainer().statistics(true)
             .jmx().enabled(true)
             .domain("x")
             .mBeanServerLookup(mBeanServerLookup)
             .addProperty("prop1", "val1")
             .addProperty("prop2", "val2");

      GlobalConfiguration configuration = builder.build();

      String toJSON = jsonWriter.toJSON(configuration);

      JsonNode node = new ObjectMapper().readTree(toJSON);
      JsonNode cacheContainer = node.get("infinispan").get("cache-container");
      JsonNode jmx = cacheContainer.get("jmx");

      assertTrue(cacheContainer.get("statistics").asBoolean());
      assertTrue(jmx.get("enabled").asBoolean());
      assertEquals("x", jmx.get("domain").asText());
      assertEquals(TestMBeanServerLookup.class.getName(), jmx.get("mbean-server-lookup").asText());
      assertEquals("val1", jmx.get("properties").get("prop1").asText());
      assertEquals("val2", jmx.get("properties").get("prop2").asText());
   }

   private Set<String> asSet(ArrayNode node) {
      return StreamSupport.stream(node.spliterator(), false).map(JsonNode::asText).collect(Collectors.toSet());
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
      public void initialize(EmbeddedCacheManager embeddedCacheManager, ConfigurationManager configurationManager, Executor executor) {
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
      public Map<String, Configuration> loadAll() {
         return null;
      }
   }
}
