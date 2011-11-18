/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.configuration.cache;

import java.beans.PropertyEditorSupport;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
public class FileCacheStoreConfigurationBuilder
      extends AbstractLockSupportCacheStoreConfigurationBuilder<FileCacheStoreConfiguration> {

   private String location;
   private long fsyncInterval;
   private FsyncMode fsyncMode;
   private int streamBufferSize;

   protected FileCacheStoreConfigurationBuilder(LoaderConfigurationBuilder builder) {
      super(builder);
   }

   public FileCacheStoreConfigurationBuilder location(String location) {
      this.location = location;
      return this;
   }

   public FileCacheStoreConfigurationBuilder fsyncInterval(long fsyncInterval) {
      this.fsyncInterval = fsyncInterval;
      return this;
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
   void validate() {
      // TODO: Customise this generated block
   }

   @Override
   FileCacheStoreConfiguration create() {
      return new FileCacheStoreConfiguration(
            lockAcquistionTimeout, lockConcurrencyLevel, location, fsyncInterval,
            fsyncMode, streamBufferSize);
   }

   // Shared with others cache stores...

   @Override
   public FileCacheStoreConfigurationBuilder purgeOnStartup(boolean purgeOnStartup) {
      super.purgeOnStartup(purgeOnStartup);
      return this;
   }

   @Override
   public FileCacheStoreConfigurationBuilder purgeSynchronously(boolean purgeSynchronously) {
      super.purgeSynchronously(purgeSynchronously);
      return this;
   }

   @Override
   public FileCacheStoreConfigurationBuilder fetchPersistentState(boolean fetchPersistentState) {
      super.fetchPersistentState(fetchPersistentState);
      return this;
   }

   @Override
   public FileCacheStoreConfigurationBuilder ignoreModifications(boolean ignoreModifications) {
      super.ignoreModifications(ignoreModifications);
      return this;
   }

   @Override
   public FileCacheStoreConfigurationBuilder lockAcquistionTimeout(long lockAcquistionTimeout) {
      super.lockAcquistionTimeout(lockAcquistionTimeout);
      return this;
   }

   @Override
   public FileCacheStoreConfigurationBuilder lockConcurrencyLevel(int lockConcurrencyLevel) {
      super.lockConcurrencyLevel(lockConcurrencyLevel);
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
