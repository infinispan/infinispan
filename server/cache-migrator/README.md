# Infinispan Cache Migrator

A utility module for migrating cache entries between remote Infinispan caches using reactive streams.

**Requirements:** Java 17+

## Features

- **Reactive streaming**: Uses RxJava 3 Flowable for efficient streaming
- **Concurrency control**: Configurable parallel operations (default: 16)
- **Batch processing**: Configurable batch size for Publisher API (default: 1000)
- **Progress tracking**: Optional callback for monitoring migration progress
- **Octet-stream support**: Works with raw byte arrays using APPLICATION_OCTET_STREAM media type
- **Type-safe API**: Generic methods support typed caches

## Command-line Usage

### Help and Version

```bash
# Display help information
java -jar infinispan-server-cache-migrator-16.2.0-SNAPSHOT.jar --help

# Display version information
java -jar infinispan-server-cache-migrator-16.2.0-SNAPSHOT.jar --version
```

### Basic Migration

```bash
java -jar infinispan-server-cache-migrator-16.2.0-SNAPSHOT.jar \
  hotrod://localhost:11222 sourceCache targetCache
```

### Migration with Old RESP Encoding

**IMPORTANT:** This option should only be used when migrating caches that were configured to be used for the RESP endpoint and are running on an Infinispan server version **before 16.2**. Starting with version 16.2, RESP encoding was changed, and this migration mode helps transition from the old encoding to the new one.

For migrating caches that were created with older RESP implementations that used different encoding, use the `--old-resp` flag. This mode attempts to deserialize values using the global marshaller. If the deserialized value is a RESP object type (HashMapBucket, ListBucket, SetBucket, SortedSetBucket, byte[], or JsonBucket), the deserialized object is used. Otherwise, the raw byte[] is used.

```bash
java -jar infinispan-server-cache-migrator-16.2.0-SNAPSHOT.jar \
  hotrod://localhost:11222 sourceCache targetCache \
  --old-resp
```

### Migration with Cache Creation

You can provide a cache configuration string to create the target cache before migration. The configuration can be in XML, JSON, or YAML format - the server will auto-detect the format.

**Default Configuration:** If no `--config` is provided, the target cache will be created with:
- Key media type: `application/octet-stream`
- Value media type: `application/x-java-object`
- Distributed cache with SYNC mode

**Configuration Merging:** When you provide a `--config`, it is read on top of the default configuration using Infinispan's `ConfigurationBuilder.read()` method. This means:
- Your configuration overrides default values
- Unspecified settings retain their defaults
- You only need to specify what you want to change

```bash
# Using XML configuration
java -jar infinispan-server-cache-migrator-16.2.0-SNAPSHOT.jar \
  hotrod://localhost:11222 sourceCache targetCache \
  --config '<distributed-cache mode="SYNC"><encoding><key media-type="application/x-protostream"/></encoding></distributed-cache>'

# Using JSON configuration
java -jar infinispan-server-cache-migrator-16.2.0-SNAPSHOT.jar \
  hotrod://localhost:11222 sourceCache targetCache \
  --config '{"distributed-cache":{"mode":"SYNC","encoding":{"key":{"media-type":"application/x-protostream"}}}}'

# Using YAML configuration
java -jar infinispan-server-cache-migrator-16.2.0-SNAPSHOT.jar \
  hotrod://localhost:11222 sourceCache targetCache \
  --config 'distributedCache:
  mode: "SYNC"
  encoding:
    key:
      mediaType: "application/x-protostream"'
```

**Note:** The configuration is passed as a string argument. The format (XML, JSON, or YAML) is auto-detected by `ParserRegistry`. Your custom configuration is merged with the defaults using `ConfigurationBuilder.read()`, so you only need to specify what you want to override.

## Programmatic Usage

### Basic Example

```java
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.server.cachemigrator.CacheMigrationService;
import org.infinispan.server.cachemigrator.CacheMigrationService.MigrationResult;

ConfigurationBuilder builder = new ConfigurationBuilder();
builder.uri("hotrod://localhost:11222");

try (RemoteCacheManager cacheManager = new RemoteCacheManager(builder.build())) {
   CacheMigrationService migrationService = new CacheMigrationService(cacheManager);

   // Migrate from sourceCache to targetCache
   MigrationResult result = migrationService.migrate("sourceCache", "targetCache");

   System.out.println("Migrated: " + result.getEntriesSuccessful());
   System.out.println("Failed: " + result.getEntriesFailed());
   System.out.println("Throughput: " + result.getThroughput() + " entries/sec");
}
```

### With Progress Tracking

```java
MigrationResult result = migrationService.migrate(
   "sourceCache",
   "targetCache",
   count -> {
      if (count % 1000 == 0) {
         System.out.println("Processed " + count + " entries...");
      }
   }
);
```

### Custom Configuration

```java
// Custom batch size and concurrency
CacheMigrationService migrationService = new CacheMigrationService(
   cacheManager,
   5000,  // batch size
   32     // concurrency
);
```

### Typed Cache Migration

```java
RemoteCache<String, Person> sourceCache = cacheManager.getCache("people");
RemoteCache<String, Person> targetCache = cacheManager.getCache("peopleBackup");

MigrationResult result = migrationService.migrateCache(sourceCache, targetCache);
```

### Old RESP Migration

**IMPORTANT:** This method should only be used when migrating caches that were configured to be used for the RESP endpoint and are running on an Infinispan server version **before 16.2**. Starting with version 16.2, RESP encoding was changed, and this migration mode helps transition from the old encoding to the new one.

For migrating caches created with older RESP encoding that used the global marshaller for serialization:

```java
// Migrates using old RESP encoding - attempts to deserialize values
// If value is a RESP type (hash, list, set, zset, string, json), uses deserialized object
// Otherwise, uses raw byte[]
MigrationResult result = migrationService.migrateOldResp("oldCache", "newCache");

// With progress tracking
MigrationResult result = migrationService.migrateOldResp(
   "oldCache",
   "newCache",
   count -> {
      if (count % 1000 == 0) {
         System.out.println("Processed " + count + " entries...");
      }
   }
);
```

### Creating Cache with Configuration

```java
// Create with custom XML configuration
String xmlConfig = "<distributed-cache mode=\"SYNC\"><encoding><key media-type=\"application/x-protostream\"/></encoding></distributed-cache>";
migrationService.createTargetCache("targetCache", xmlConfig);

// Create with custom JSON configuration
String jsonConfig = "{\"distributed-cache\": {\"mode\": \"SYNC\"}}";
migrationService.createTargetCache("targetCache", jsonConfig);

// Create with custom YAML configuration
String yamlConfig = "distributedCache:\n  mode: \"SYNC\"";
migrationService.createTargetCache("targetCache", yamlConfig);

// Create with default configuration (octet-stream keys, x-java-object values)
migrationService.createTargetCache("targetCache", null);

// Or use getOrCreateTargetCache to avoid errors if cache exists
migrationService.getOrCreateTargetCache("targetCache", xmlConfig);
migrationService.getOrCreateTargetCache("targetCache", null); // with defaults
```

## Maven Dependency

Add this to your `pom.xml`:

```xml
<dependency>
   <groupId>org.infinispan</groupId>
   <artifactId>infinispan-server-cache-migrator</artifactId>
   <version>16.2.0-SNAPSHOT</version>
</dependency>
```

## Migration Result

The `MigrationResult` object provides:

- `getEntriesProcessed()`: Total number of entries processed
- `getEntriesFailed()`: Number of failed entries
- `getEntriesSuccessful()`: Number of successfully migrated entries
- `getDurationMs()`: Migration duration in milliseconds
- `getThroughput()`: Entries per second
- `toString()`: Formatted summary of the migration

## Implementation Details

- **CLI Framework**: Picocli for robust command-line parsing with auto-generated help
- **Streaming**: Uses HotRod client's `publishEntries()` API for efficient streaming
- **Reactive**: Leverages RxJava 3's `flatMapCompletable` for concurrent async operations
- **Configurable**: Adjustable concurrency limit to control resource usage
- **Non-blocking**: Each entry is migrated using `putAsync()` for non-blocking operations
- **Resilient**: Failures are logged but don't stop the migration process
- **Admin API**: Supports cache creation via `RemoteCacheManagerAdmin` API
- **Format Detection**: Auto-detects configuration format (XML, JSON, YAML) on the server side

## Configuration Examples

Example configuration files are provided in the `examples/` directory that can be used as reference or with shell command substitution:

- `cache-config.xml` - XML format configuration
- `cache-config.json` - JSON format configuration
- `cache-config.yaml` - YAML format configuration

All three examples define the same cache configuration:
- Distributed cache with SYNC mode
- Protostream encoding for keys and values
- Maximum count of 1,000,000 entries
- Statistics enabled
- Indexing disabled

### Using Configuration Files with Shell Substitution

While the tool expects configuration as a string argument, you can use shell command substitution to read from files:

```bash
# Using cat to read from file (works with any format - XML, JSON, or YAML)
java -jar infinispan-server-cache-migrator-16.2.0-SNAPSHOT.jar \
  hotrod://localhost:11222 sourceCache targetCache \
  --config "$(cat examples/cache-config.xml)"

# Or with JSON
java -jar infinispan-server-cache-migrator-16.2.0-SNAPSHOT.jar \
  hotrod://localhost:11222 sourceCache targetCache \
  --config "$(cat examples/cache-config.json)"

# Or with YAML
java -jar infinispan-server-cache-migrator-16.2.0-SNAPSHOT.jar \
  hotrod://localhost:11222 sourceCache targetCache \
  --config "$(cat examples/cache-config.yaml)"
```
