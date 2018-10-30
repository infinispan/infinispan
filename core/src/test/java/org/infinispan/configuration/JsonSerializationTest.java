package org.infinispan.configuration;

import static org.testng.AssertJUnit.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import org.infinispan.Version;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.configuration.JsonReader;
import org.infinispan.commons.configuration.JsonWriter;
import org.infinispan.commons.equivalence.ByteArrayEquivalence;
import org.infinispan.commons.equivalence.IdentityEquivalence;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.conflict.MergePolicy;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.ch.impl.AffinityPartitioner;
import org.infinispan.distribution.ch.impl.TopologyAwareSyncConsistentHashFactory;
import org.infinispan.distribution.group.Grouper;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.interceptors.BaseAsyncInterceptor;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(testName = "config.JsonSerializationTest", groups = "functional")
public class JsonSerializationTest extends AbstractInfinispanTest {

   private JsonReader jsonReader = new JsonReader();
   private JsonWriter jsonWriter = new JsonWriter();

   @Test
   public void testMinimalConfiguration() {
      Configuration minimal = new ConfigurationBuilder().build();
      testJsonConversion(minimal);
   }

   @Test
   public void testComplexConfiguration() {
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      configurationBuilder
            .unsafe().unreliableReturnValues(true)
            .deadlockDetection().enabled(true).spinDuration(1, TimeUnit.HOURS)
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
            .jmxStatistics().enable()
            .transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .lockingMode(LockingMode.PESSIMISTIC).useSynchronization(false).transactionProtocol(TransactionProtocol.DEFAULT)
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
            .dataContainer()
            .addProperty("prop1", "value1")
            .keyEquivalence(new IdentityEquivalence<>())
            .valueEquivalence(new ByteArrayEquivalence())
            .memory().size(123).storageType(StorageType.OBJECT).addressCount(12).evictionStrategy(EvictionStrategy.FIFO);

      Configuration configuration = configurationBuilder.build();

      testJsonConversion(configuration);
   }

   @Test
   public void testLatestVersion() throws IOException {
      ParserRegistry parserRegistry = new ParserRegistry();
      ConfigurationBuilderHolder builderHolder = parserRegistry.parse(loadLatestVersionTest());
      testConfigurations(builderHolder);
   }

   @Test
   public void testResourcesConfiguration() throws FileNotFoundException {
      ParserRegistry parserRegistry = new ParserRegistry();
      File[] resourceConfigs = getResourceConfigs();
      for (File f : resourceConfigs) {
         if (f.isDirectory()) continue;
         ConfigurationBuilderHolder builderHolder = parserRegistry.parse(new FileInputStream(f));
         testConfigurations(builderHolder);
      }
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

   private InputStream loadLatestVersionTest() throws IOException {
      String majorMinor = Version.getMajorMinor();
      ClassLoader loader = this.getClass().getClassLoader();
      String testFile = "configs/unified/" + majorMinor + ".xml";
      URL resource = loader.getResource(testFile);
      if (resource == null) {
         Assert.fail(String.format("Unable to find test configuration file '%s'", testFile));
      }
      return resource.openStream();
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
}
