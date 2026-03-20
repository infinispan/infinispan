package org.infinispan.server.cachemigrator;

import java.util.concurrent.Callable;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Cache Migrator command-line utility to migrate entries from one cache to another.
 *
 * @author Infinispan
 * @since 16.2
 */
@Command(
      name = "CacheMigrator",
      description = "Migrate cache entries from one remote cache to another",
      mixinStandardHelpOptions = true,
      version = "16.2.0-SNAPSHOT"
)
public class CacheMigrator implements Callable<Integer> {

   private static final Log log = LogFactory.getLog(CacheMigrator.class);

   @Parameters(index = "0", description = "HotRod server URL (e.g., hotrod://localhost:11222)")
   private String serverUrl;

   @Parameters(index = "1", description = "Name of the source cache")
   private String sourceCache;

   @Parameters(index = "2", description = "Name of the target cache")
   private String targetCache;

   @Option(names = {"--config", "-c"},
           description = "Configuration string for target cache (XML, JSON, or YAML). " +
                         "The server will auto-detect the format. " +
                         "If provided, the target cache will be created before migration.")
   private String configString;

   @Option(names = {"--old-resp"},
           description = "Migrate from old RESP encoding (pre-16.2). " +
                         "IMPORTANT: Only use this when migrating caches configured for the RESP endpoint " +
                         "from Infinispan versions before 16.2. " +
                         "Attempts to deserialize values using the global marshaller. " +
                         "If the value is a RESP type (hash, list, set, zset, string, json), " +
                         "the deserialized object is used. Otherwise, the raw byte[] is used.")
   private boolean oldResp;

   public static void main(String[] args) {
      int exitCode = new CommandLine(new CacheMigrator()).execute(args);
      System.exit(exitCode);
   }

   @Override
   public Integer call() {
      log.infof("Cache Migrator");
      log.infof("==============");
      log.infof("Server URL: %s", serverUrl);
      log.infof("Source cache: %s", sourceCache);
      log.infof("Target cache: %s", targetCache);
      if (configString != null) {
         log.infof("Target cache will be created with provided configuration");
         log.debugf("Configuration: %s", configString);
      }
      if (oldResp) {
         log.infof("Old RESP mode enabled: will attempt to deserialize values");
      }

      try {
         migrate(serverUrl, sourceCache, targetCache, configString, oldResp);
         return 0;
      } catch (Exception e) {
         log.errorf(e, "Migration failed: %s", e.getMessage());
         return 1;
      }
   }

   private static void migrate(String serverUrl, String sourceCacheName, String targetCacheName,
                                String configString, boolean oldResp) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.uri(serverUrl);

      try (RemoteCacheManager cacheManager = new RemoteCacheManager(builder.build())) {
         CacheMigrationService migrationService = new CacheMigrationService(cacheManager);

         // If configuration is provided, create the cache first
         if (configString != null) {
            log.infof("Creating target cache '%s' with provided configuration...", targetCacheName);
            migrationService.createTargetCache(targetCacheName, configString);
            log.infof("Target cache created successfully");
         }

         log.infof("Starting migration...");

         CacheMigrationService.MigrationResult result = oldResp
               ? migrationService.migrateOldResp(
                     sourceCacheName,
                     targetCacheName,
                     count -> {
                        if (count % 1000 == 0) {
                           log.infof("Processed %d entries...", count);
                        }
                     }
               )
               : migrationService.migrate(
                     sourceCacheName,
                     targetCacheName,
                     count -> {
                        if (count % 1000 == 0) {
                           log.infof("Processed %d entries...", count);
                        }
                     }
               );

         log.infof("Migration completed!");
         log.infof("Total entries: %d", result.entriesProcessed());
         log.infof("Successful entries: %d", result.getEntriesSuccessful());
         log.infof("Failed entries: %d", result.entriesFailed());
         log.infof("Duration: %d ms", result.durationMs());
         log.infof("Throughput: %d entries/sec", result.getThroughput());
      }
   }
}
