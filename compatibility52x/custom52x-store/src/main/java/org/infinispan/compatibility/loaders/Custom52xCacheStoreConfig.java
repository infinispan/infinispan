package org.infinispan.compatibility.loaders;

import org.infinispan.loaders.LockSupportCacheStoreConfig;

import java.beans.PropertyEditorSupport;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class Custom52xCacheStoreConfig extends LockSupportCacheStoreConfig {

   private static final long serialVersionUID = 1551092386868095926L;

   private String location = "Infinispan-Custom52xCacheStore";
   private int streamBufferSize = 8192;
   private FsyncMode fsyncMode = FsyncMode.DEFAULT;
   private long fsyncInterval = 1000;

   public Custom52xCacheStoreConfig() {
      setCacheLoaderClassName(Custom52xCacheStore.class.getName());
   }

   public String getLocation() {
      return location;
   }

   /**
    * @deprecated The visibility of this will be reduced, use {@link #location(String)}
    */
   @Deprecated
   public void setLocation(String location) {
      testImmutability("location");
      this.location = location;
   }

   public Custom52xCacheStoreConfig location(String location) {
      setLocation(location);
      return this;
   }

   public int getStreamBufferSize() {
      return streamBufferSize;
   }

   /**
    * @deprecated The visibility of this will be reduced, use {@link #streamBufferSize(int)} instead
    */
   @Deprecated
   public void setStreamBufferSize(int streamBufferSize) {
      testImmutability("streamBufferSize");
      this.streamBufferSize = streamBufferSize;
   }

   public Custom52xCacheStoreConfig streamBufferSize(int streamBufferSize) {
      setStreamBufferSize(streamBufferSize);
      return this;
   }

   // Method overrides below are used to make configuration more fluent.

   @Override
   public Custom52xCacheStoreConfig purgeOnStartup(Boolean purgeOnStartup) {
      super.purgeOnStartup(purgeOnStartup);
      return this;
   }

   @Override
   public Custom52xCacheStoreConfig purgeSynchronously(Boolean purgeSynchronously) {
      super.purgeSynchronously(purgeSynchronously);
      return this;
   }

   @Override
   public Custom52xCacheStoreConfig fetchPersistentState(Boolean fetchPersistentState) {
      super.fetchPersistentState(fetchPersistentState);
      return this;
   }

   @Override
   public Custom52xCacheStoreConfig ignoreModifications(Boolean ignoreModifications) {
      super.ignoreModifications(ignoreModifications);
      return this;
   }

   public long getFsyncInterval() {
      return fsyncInterval;
   }

   // TODO: This should be private since they should only be used for XML parsing, defer to XML changes for ISPN-1065
   public void setFsyncInterval(long fsyncInterval) {
      this.fsyncInterval = fsyncInterval;
   }

   public Custom52xCacheStoreConfig fsyncInterval(long fsyncInterval) {
      setFsyncInterval(fsyncInterval);
      return this;
   }

   public FsyncMode getFsyncMode() {
      return fsyncMode;
   }

   // TODO: This should be private since they should only be used for XML parsing, defer to XML changes for ISPN-1065
   public void setFsyncMode(FsyncMode fsyncMode) {
      this.fsyncMode = fsyncMode;
   }

   public Custom52xCacheStoreConfig fsyncMode(FsyncMode fsyncMode) {
      setFsyncMode(fsyncMode);
      return this;
   }

   public static enum FsyncMode {
      DEFAULT, PER_WRITE, PERIODIC
   }

   /**
    * Property editor for fsync mode configuration. It's automatically
    * registered the {@link java.beans.PropertyEditorManager} so that it can
    * transform text based modes into its corresponding enum value.
    */
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
