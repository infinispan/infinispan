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
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.infinispan.config.parsing.XmlConfigHelper;
import org.infinispan.util.TypedProperties;

/**
 * File cache store configuration builder
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class FileCacheStoreConfigurationBuilder extends AbstractLoaderConfigurationBuilder<FileCacheStoreConfiguration> {

   private String location = "Infinispan-FileCacheStore";
   private long fsyncInterval = TimeUnit.SECONDS.toMillis(1);
   private FsyncMode fsyncMode = FsyncMode.DEFAULT;
   private int streamBufferSize = 8192;
   // TODO: Sort out this duplication with LoaderConfigurationBuilder
   private boolean fetchPersistentState = false;
   private boolean ignoreModifications = false;
   private boolean purgeOnStartup = false;
   private int purgerThreads = 1;
   private boolean purgeSynchronously = false;
   private int lockConcurrencyLevel;
   private long lockAcquistionTimeout;
   private Properties properties = new Properties();

   protected FileCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
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
   public void validate() {
   }

   // Shared with others cache stores...

   public FileCacheStoreConfigurationBuilder purgeOnStartup(boolean purgeOnStartup) {
      this.purgeOnStartup = purgeOnStartup;
      return this;
   }

   public FileCacheStoreConfigurationBuilder purgeSynchronously(boolean purgeSynchronously) {
      this.purgeSynchronously = purgeSynchronously;
      return this;
   }

   public FileCacheStoreConfigurationBuilder purgerThreads(int i) {
      this.purgerThreads = i;
      return this;
   }

   public FileCacheStoreConfigurationBuilder fetchPersistentState(boolean fetchPersistentState) {
      this.fetchPersistentState = fetchPersistentState;
      return this;
   }

   public FileCacheStoreConfigurationBuilder ignoreModifications(boolean ignoreModifications) {
      this.ignoreModifications = ignoreModifications;
      return this;
   }

   public FileCacheStoreConfigurationBuilder lockAcquistionTimeout(long lockAcquistionTimeout) {
      this.lockAcquistionTimeout = lockAcquistionTimeout;
      return this;
   }

   public FileCacheStoreConfigurationBuilder lockAcquistionTimeout(long lockAcquistionTimeout, TimeUnit unit) {
      return lockAcquistionTimeout(unit.toMillis(lockAcquistionTimeout));
   }

   public FileCacheStoreConfigurationBuilder lockConcurrencyLevel(int lockConcurrencyLevel) {
      this.lockConcurrencyLevel = lockConcurrencyLevel;
      return this;
   }

   @Override
   public AbstractLoaderConfigurationBuilder<FileCacheStoreConfiguration> withProperties(Properties p) {
      this.properties = p;
      // TODO: Remove this and any sign of properties when switching to new cache store configs
      XmlConfigHelper.setValues(this, properties, false, true);
      return this;
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
      fetchPersistentState = template.fetchPersistentState();
      fsyncInterval = template.fsyncInterval();
      fsyncMode = template.fsyncMode();
      ignoreModifications = template.ignoreModifications();
      location = template.location();
      lockAcquistionTimeout = template.lockAcquistionTimeout();
      lockConcurrencyLevel = template.lockConcurrencyLevel();
      properties = template.properties();
      purgeOnStartup = template.purgeOnStartup();
      purgeSynchronously = template.purgeSynchronously();
      streamBufferSize = template.streamBufferSize();

      this.async.read(template.async());
      this.singletonStore.read(template.singletonStore());

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
