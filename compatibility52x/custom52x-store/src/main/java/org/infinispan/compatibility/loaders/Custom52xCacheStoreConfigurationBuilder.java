package org.infinispan.compatibility.loaders;

import org.infinispan.config.parsing.XmlConfigHelper;
import org.infinispan.configuration.cache.AbstractLockSupportStoreConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.util.TypedProperties;

import java.beans.PropertyEditorSupport;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class Custom52xCacheStoreConfigurationBuilder extends AbstractLockSupportStoreConfigurationBuilder<Custom52xCacheStoreConfiguration, Custom52xCacheStoreConfigurationBuilder> {

   private String location = "Infinispan-Custom52xCacheStore";
   private long fsyncInterval = TimeUnit.SECONDS.toMillis(1);
   private FsyncMode fsyncMode = FsyncMode.DEFAULT;
   private int streamBufferSize = 8192;

   public Custom52xCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public Custom52xCacheStoreConfigurationBuilder self() {
      return this;
   }

   public Custom52xCacheStoreConfigurationBuilder location(String location) {
      this.location = location;
      return this;
   }

   public Custom52xCacheStoreConfigurationBuilder fsyncInterval(long fsyncInterval) {
      this.fsyncInterval = fsyncInterval;
      return this;
   }

   public Custom52xCacheStoreConfigurationBuilder fsyncInterval(long fsyncInterval, TimeUnit unit) {
      return fsyncInterval(unit.toMillis(fsyncInterval));
   }

   public Custom52xCacheStoreConfigurationBuilder fsyncMode(FsyncMode fsyncMode) {
      this.fsyncMode = fsyncMode;
      return this;
   }

   public Custom52xCacheStoreConfigurationBuilder streamBufferSize(int streamBufferSize) {
      this.streamBufferSize = streamBufferSize;
      return this;
   }

   @Override
   public Custom52xCacheStoreConfigurationBuilder withProperties(Properties p) {
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
   public Custom52xCacheStoreConfiguration create() {
      return new Custom52xCacheStoreConfiguration(location, fsyncInterval, fsyncMode,
                                                                                streamBufferSize, lockAcquistionTimeout, lockConcurrencyLevel,
                                                                                purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState,
                                                                                ignoreModifications, TypedProperties.toTypedProperties(properties),
                                                                                async.create(), singletonStore.create());
   }

   @Override
   public Custom52xCacheStoreConfigurationBuilder read(Custom52xCacheStoreConfiguration template) {
      // Custom52xCacheStore-specific configuration
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
      return "Custom52xCacheStoreConfigurationBuilder{" +
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
