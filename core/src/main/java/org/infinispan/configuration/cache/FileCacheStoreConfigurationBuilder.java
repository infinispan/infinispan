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

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.infinispan.util.TypedProperties;

/**
 * // TODO: Document this
 * 
 * @author Galder Zamarreño
 * @since // TODO
 */
public class FileCacheStoreConfigurationBuilder extends AbstractLoaderConfigurationBuilder<FileCacheStoreConfiguration> {

   private String location = "Infinispan-FileCacheStore";
   private long fsyncInterval = TimeUnit.SECONDS.toMillis(1);
   private FsyncMode fsyncMode = FsyncMode.DEFAULT;
   private int streamBufferSize = 8192;
   private boolean fetchPersistentState = false;
   private boolean ignoreModifications = false;
   private boolean purgeOnStartup = false;
   private boolean purgeSynchronously = false;
   private int lockConcurrencyLevel;
   private long lockAcquistionTimeout;
   private final AsyncLoaderConfigurationBuilder async;
   private final SingletonStoreConfigurationBuilder singletonStore;
   private Properties properties = new Properties();

   protected FileCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
      this.async = new AsyncLoaderConfigurationBuilder(this);
      this.singletonStore = new SingletonStoreConfigurationBuilder(this);
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

   // Shared with others cache stores...

   public FileCacheStoreConfigurationBuilder purgeOnStartup(boolean purgeOnStartup) {
      this.purgeOnStartup = purgeOnStartup;
      return this;
   }

   public FileCacheStoreConfigurationBuilder purgeSynchronously(boolean purgeSynchronously) {
      this.purgeSynchronously = purgeSynchronously;
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

   public FileCacheStoreConfigurationBuilder lockConcurrencyLevel(int lockConcurrencyLevel) {
      this.lockConcurrencyLevel = lockConcurrencyLevel;
      return this;
   }

   public static enum FsyncMode {
      DEFAULT, PER_WRITE, PERIODIC
   }

   @Override
   FileCacheStoreConfiguration create() {
      return new FileCacheStoreConfiguration(location, fsyncInterval, fsyncMode, streamBufferSize, lockAcquistionTimeout, lockConcurrencyLevel, purgeOnStartup, purgeSynchronously, fetchPersistentState, ignoreModifications, TypedProperties.toTypedProperties(properties), async.create(), singletonStore.create());
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

}
