package org.infinispan.configuration.cache;

import java.beans.PropertyEditorSupport;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.util.TypedProperties;
import org.infinispan.config.parsing.XmlConfigHelper;

/**
 * File cache store configuration builder
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class FileCacheStoreConfigurationBuilder extends AbstractLockSupportStoreConfigurationBuilder<FileCacheStoreConfiguration, FileCacheStoreConfigurationBuilder> {

   private String location = "Infinispan-FileCacheStore";
   private long fsyncInterval = TimeUnit.SECONDS.toMillis(1);
   private FsyncMode fsyncMode = FsyncMode.DEFAULT;
   private int streamBufferSize = 8192;
   private int maxEntries = -1;
   private boolean deprecatedBucketFormat = false;

   public FileCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public FileCacheStoreConfigurationBuilder self() {
      return this;
   }

   public FileCacheStoreConfigurationBuilder location(String location) {
      this.location = location;
      return this;
   }

   @Deprecated
   public FileCacheStoreConfigurationBuilder fsyncInterval(long fsyncInterval) {
      this.fsyncInterval = fsyncInterval;
      return this;
   }

   @Deprecated
   public FileCacheStoreConfigurationBuilder fsyncInterval(long fsyncInterval, TimeUnit unit) {
      return fsyncInterval(unit.toMillis(fsyncInterval));
   }

   @Deprecated
   public FileCacheStoreConfigurationBuilder fsyncMode(FsyncMode fsyncMode) {
      this.fsyncMode = fsyncMode;
      return this;
   }

   @Deprecated
   public FileCacheStoreConfigurationBuilder streamBufferSize(int streamBufferSize) {
      this.streamBufferSize = streamBufferSize;
      return this;
   }

   /**
    * In order to speed up lookups, the file cache store keeps an index
    * of keys and their corresponding position in the file. To avoid this
    * index resulting in memory consumption problems, this cache store can
    * bounded by a maximum number of entries that it stores. If this limit is
    * exceeded, entries are removed permanently using the LRU algorithm both
    * from the in-memory index and the underlying file based cache store.
    *
    * So, setting a maximum limit only makes sense when Infinispan is used as
    * a cache, whose contents can be recomputed or they can be retrieved from
    * the authoritative data store.
    *
    * If this maximum limit is set when the Infinispan is used as an
    * authoritative data store, it could lead to data loss, and hence it's
    * not recommended for this use case.
    */
   public FileCacheStoreConfigurationBuilder maxEntries(int maxEntries) {
      this.maxEntries = maxEntries;
      return this;
   }

   /**
    * The format in which the file cache store keeps data in the file system
    * has changed in Infinispan 6.0. If Infinispan detects that a file cache
    * store exists with pre-Infinispan 6.0 format, it will back up the data
    * to a folder with the same location adding a '.backup' extension to it
    * in order to differentiate it from the main location.
    *
    * Setting deprecated format to true flag enables caches to be read from
    * that backup location without the risk of data being upgrade to new format.
    * The user does not need to need to change the location in order to read
    * the old backup. Infinispan internally can locate the old backup location.
    */
   public FileCacheStoreConfigurationBuilder deprecatedBucketFormat(boolean enable) {
      this.deprecatedBucketFormat = enable;
      return this;
   }

   @Override
   @Deprecated
   public FileCacheStoreConfigurationBuilder withProperties(Properties p) {
      this.properties = p;
      // TODO: Remove this and any sign of properties when switching to new cache store configs
      XmlConfigHelper.setValues(this, properties, false, true);
      return this;
   }

   @Override
   public void validate() {
   }

   @Deprecated
   public static enum FsyncMode {
      DEFAULT, PER_WRITE, PERIODIC
   }

   @Override
   public FileCacheStoreConfiguration create() {
      return new FileCacheStoreConfiguration(location, maxEntries, deprecatedBucketFormat,
            fsyncInterval, fsyncMode,
            streamBufferSize, lockAcquistionTimeout, lockConcurrencyLevel,
            purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState,
            ignoreModifications, TypedProperties.toTypedProperties(properties),
            async.create(), singletonStore.create());
   }

   @Override
   public FileCacheStoreConfigurationBuilder read(FileCacheStoreConfiguration template) {
      // FileCacheStore-specific configuration
      fsyncInterval = template.fsyncInterval();
      fsyncMode = template.fsyncMode();
      location = template.location();
      maxEntries = template.maxEntries();
      deprecatedBucketFormat = template.deprecatedBucketFormat();
      streamBufferSize = template.streamBufferSize();

      // AbstractLockSupportCacheStore-specific configuration
      lockAcquistionTimeout = template.lockAcquistionTimeout();
      lockConcurrencyLevel = template.lockConcurrencyLevel();

      // AbstractStore-specific configuration
      fetchPersistentState = template.fetchPersistentState();
      ignoreModifications = template.ignoreModifications();
      properties = template.properties();
      purgeOnStartup = template.purgeOnStartup();
      purgeSynchronously = template.purgeSynchronously();
      async.read(template.async());
      singletonStore.read(template.singletonStore());

      return this;
   }

   @Override
   public String toString() {
      return "FileCacheStoreConfigurationBuilder{" +
            "fetchPersistentState=" + fetchPersistentState +
            ", location='" + location + '\'' +
            ", maxEntries=" + maxEntries +
            ", deprecatedBucketFormat=" + deprecatedBucketFormat +
            ", fsyncInterval=" + fsyncInterval +
            ", fsyncMode=" + fsyncMode +
            ", streamBufferSize=" + streamBufferSize +
            ", ignoreModifications=" + ignoreModifications +
            ", purgeOnStartup=" + purgeOnStartup +
            ", purgerThreads=" + purgerThreads +
            ", purgeSynchronously=" + purgeSynchronously +
            ", lockConcurrencyLevel=" + lockConcurrencyLevel +
            ", lockAcquistionTimeout=" + lockAcquistionTimeout +
            ", properties=" + properties +
            ", async=" + async +
            ", singletonStore=" + singletonStore +
            '}';
   }

   // IMPORTANT! Below is a temporary measure to convert properties into
   // instances of the right class. Please remove when switching cache store
   // config over to new config.

   /**
    * Property editor for fsync mode configuration. It's automatically
    * registered the {@link java.beans.PropertyEditorManager} so that it can
    * transform text based modes into its corresponding enum value.
    */
   @Deprecated
   public static class FsyncModeEditor extends PropertyEditorSupport {
      private FsyncMode mode;

      @Override
      public void setAsText(String text) throws IllegalArgumentException {
         if (text.equals("default"))
            mode = FsyncMode.DEFAULT;
         else if (text.equals("perWrite"))
            mode = FsyncMode.PER_WRITE;
         else if (text.equals("periodic"))
            mode = FsyncMode.PERIODIC;
         else
            throw new IllegalArgumentException("Unknown fsyncMode value: " + text);
      }

      @Override
      public Object getValue() {
         return mode;
      }
   }
}
