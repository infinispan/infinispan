package org.infinispan.loaders.file;

import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.loaders.LockSupportCacheStoreConfig;
import org.infinispan.loaders.decorators.AsyncStoreConfig;

/**
 * Configures {@link org.infinispan.loaders.file.FileCacheStore}.  This allows you to tune a number of characteristics
 * of the {@link FileCacheStore}.
 * <p/>
 *    <ul>
 *       <li><tt>location</tt> - a location on disk where the store can write internal files.  This defaults to
 * <tt>Infinispan-FileCacheStore</tt> in the current working directory.</li>
 *       <li><tt>purgeSynchronously</tt> - whether {@link org.infinispan.loaders.CacheStore#purgeExpired()} calls happen
 * synchronously or not.  By default, this is set to <tt>false</tt>.</li>
 *       <li><tt>purgerThreads</tt> - number of threads to use when purging.  Defaults to <tt>1</tt> if <tt>purgeSynchronously</tt>
 * is <tt>true</tt>, ignored if <tt>false</tt>.</li>
 *    <li><tt>streamBufferSize</tt> - when writing state to disk, a buffered stream is used.  This
 * parameter allows you to tune the buffer size.  Larger buffers are usually faster but take up more (temporary) memory,
 * resulting in more gc. By default, this is set to <tt>8192</tt>.</li>
 *    <li><tt>lockConcurrencyLevel</tt> - locking granularity is per file bucket.  This setting defines the number of
 * shared locks to use.  The more locks you have, the better your concurrency will be, but more locks take up more
 * memory. By default, this is set to <tt>2048</tt>.</li>
 *    <li><tt>lockAcquistionTimeout</tt> - the length of time, in milliseconds, to wait for locks
 * before timing out and throwing an exception.  By default, this is set to <tt>60000</tt>.</li>
 * </ul>
 *
 * @author Manik Surtani
 * @autor Vladimir Blagojevic
 * @since 4.0
 */
public class FileCacheStoreConfig extends LockSupportCacheStoreConfig {

   private static final long serialVersionUID = 1551092386868095926L;
   
   String location = "Infinispan-FileCacheStore";
   private int streamBufferSize = 8192;

   public FileCacheStoreConfig() {
      setCacheLoaderClassName(FileCacheStore.class.getName());
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

   public FileCacheStoreConfig location(String location) {
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
      testImmutability("steamBufferSize");
      this.streamBufferSize = streamBufferSize;
   }

   public FileCacheStoreConfig streamBufferSize(int streamBufferSize) {
      setStreamBufferSize(streamBufferSize);
      return this;
   }

   // Method overrides below are used to make configuration more fluent.

   @Override
   public FileCacheStoreConfig purgeOnStartup(Boolean purgeOnStartup) {
      super.purgeOnStartup(purgeOnStartup);
      return this;
   }

   @Override
   public FileCacheStoreConfig purgeSynchronously(Boolean purgeSynchronously) {
      super.purgeSynchronously(purgeSynchronously);
      return this;
   }

   @Override
   public FileCacheStoreConfig fetchPersistentState(Boolean fetchPersistentState) {
      super.fetchPersistentState(fetchPersistentState);
      return this;
   }

   @Override
   public FileCacheStoreConfig ignoreModifications(Boolean ignoreModifications) {
      super.ignoreModifications(ignoreModifications);
      return this;
   }
}
