/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.loaders.file;

import org.infinispan.loaders.LockSupportCacheStoreConfig;
import java.beans.PropertyEditorSupport;

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
 *    <li><tt>fsyncMode</tt> - configures how the file changes will be
 * synchronized with the underlying file system. This property has three
 * possible values (The default mode configured is <tt>default</tt>):
 *       <ul>
 *          <li><tt>default</tt> - means that the file system will be
 *       synchronized when the OS buffer is full or when the bucket is read.</li>
 *          <li><tt>perWrite</tt> - configures the file cache store to sync up
 *       changes after each write request</li>
 *          <li><tt>periodic</tt> - enables sync operations to happen as per a
 *       defined interval, or when the bucket is about to be read.</li>
 *       </ul>
 *   <li><tt>fsyncInterval</tt> - specifies the time after which the file
 * changes in the cache need to be flushed. This option has only effect when
 * <tt>periodic</tt> fsync mode is in use. The default fsync interval is 1
 * second.</li>
 *
 * </ul>
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @author Galder Zamarreño
 * @since 4.0
 */
public class FileCacheStoreConfig extends LockSupportCacheStoreConfig {

   private static final long serialVersionUID = 1551092386868095926L;
   
   private String location = "Infinispan-FileCacheStore";
   private int streamBufferSize = 8192;
   private FsyncMode fsyncMode = FsyncMode.DEFAULT;
   private long fsyncInterval = 1000;

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
      testImmutability("streamBufferSize");
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

   public long getFsyncInterval() {
      return fsyncInterval;
   }

   // TODO: This should be private since they should only be used for XML parsing, defer to XML changes for ISPN-1065
   public void setFsyncInterval(long fsyncInterval) {
      this.fsyncInterval = fsyncInterval;
   }

   public FileCacheStoreConfig fsyncInterval(long fsyncInterval) {
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

   public FileCacheStoreConfig fsyncMode(FsyncMode fsyncMode) {
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
