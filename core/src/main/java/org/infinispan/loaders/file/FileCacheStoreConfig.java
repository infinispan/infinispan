package org.infinispan.loaders.file;

import org.infinispan.config.ConfigurationElement;
import org.infinispan.config.ConfigurationElements;
import org.infinispan.config.ConfigurationProperty;
import org.infinispan.loaders.LockSupportCacheStoreConfig;

/**
 * Configures {@link org.infinispan.loaders.file.FileCacheStore}.  This allows you to tune a number of characteristics
 * of the {@link FileCacheStore}.
 * <p/>
 * <ul> <li><tt>location</tt> - a location on disk where the store can write internal files.  This defaults to
 * <tt>Infinispan-FileCacheStore</tt> in the current working directory.</li> <li><tt>purgeSynchronously</tt> - whether
 * {@link org.infinispan.loaders.CacheStore#purgeExpired()} calls happen synchronously or not.  By default, this is set
 * to <tt>false</tt>.</li> <li><tt>streamBufferSize</tt> - when writing state to disk, a buffered stream is used.  This
 * parameter allows you to tune the buffer size.  Larger buffers are usually faster but take up more (temporary) memory,
 * resulting in more gc. By default, this is set to <tt>8192</tt>.</li> <li><tt>lockConcurrencyLevel</tt> - locking
 * granularity is per file bucket.  This setting defines the number of shared locks to use.  The more locks you have,
 * the better your concurrency will be, but more locks take up more memory. By default, this is set to
 * <tt>2048</tt>.</li> <li><tt>lockAcquistionTimeout</tt> - the length of time, in milliseconds, to wait for locks
 * before timing out and throwing an exception.  By default, this is set to <tt>60000</tt>.</li> </ul>
 *
 * @author Manik Surtani
 * @since 4.0
 */
@ConfigurationElements(elements = {
         @ConfigurationElement(name = "loader", parent = "loaders", description = "org.infinispan.loaders.file.FileCacheStore"),
         @ConfigurationElement(name = "properties", parent = "loader") })
public class FileCacheStoreConfig extends LockSupportCacheStoreConfig {
   String location = "Infinispan-FileCacheStore";
   private int streamBufferSize = 8192;

   public FileCacheStoreConfig() {
      setCacheLoaderClassName(FileCacheStore.class.getName());
   }

   public String getLocation() {
      return location;
   }

   @ConfigurationProperty(name="location",
            parentElement="properties",
            description="Path to file")
   public void setLocation(String location) {
      testImmutability("location");
      this.location = location;
   }


   public int getStreamBufferSize() {
      return streamBufferSize;
   }

   public void setStreamBufferSize(int streamBufferSize) {
      testImmutability("steamBufferSize");
      this.streamBufferSize = streamBufferSize;
   }
}
