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

   public FileCacheStoreConfigurationBuilder fsyncInterval(long fsyncInterval) {
      this.fsyncInterval = fsyncInterval;
      return this;
   }

   public FileCacheStoreConfigurationBuilder fsyncInterval(long fsyncInterval, TimeUnit unit) {
      return fsyncInterval(unit.toMillis(fsyncInterval));
   }

   public FileCacheStoreConfigurationBuilder fsyncMode(FsyncMode fsyncMode) {
      this.fsyncMode = fsyncMode;
      return this;
   }

   public FileCacheStoreConfigurationBuilder streamBufferSize(int streamBufferSize) {
      this.streamBufferSize = streamBufferSize;
      return this;
   }

   @Override
   public FileCacheStoreConfigurationBuilder withProperties(Properties p) {
      this.properties = p;
      // TODO: Remove this and any sign of properties when switching to new cache store configs
      XmlConfigHelper.setValues(this, properties, false, true);
      return this;
   }

   @Override
   public void validate() {
   }

   public static enum FsyncMode {
      DEFAULT, PER_WRITE, PERIODIC
   }

   @Override
   public FileCacheStoreConfiguration create() {
      return new FileCacheStoreConfiguration(location, fsyncInterval, fsyncMode,
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
