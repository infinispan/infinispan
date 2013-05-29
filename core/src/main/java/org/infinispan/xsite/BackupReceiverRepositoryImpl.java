/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.xsite;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.protocols.relay.SiteAddress;
import org.jgroups.protocols.relay.SiteUUID;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Listener
public class BackupReceiverRepositoryImpl implements BackupReceiverRepository {

   private static Log log = LogFactory.getLog(BackupReceiverRepositoryImpl.class);

   private final ConcurrentMap<SiteCachePair, BackupReceiver> backupReceivers = new ConcurrentHashMap<SiteCachePair, BackupReceiver>();

   public EmbeddedCacheManager cacheManager;

   @Inject
   public void setup(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

   @Start
   public void start() {
      cacheManager.addListener(this);
   }

   @Stop
   public void stop() {
      cacheManager.removeListener(this);
   }

   @CacheStopped
   public void cacheStopped(CacheStoppedEvent cse) {
      log.debugf("Processing cache stop: %s. Cache name: '%s'", cse, cse.getCacheName());
      for (SiteCachePair scp : backupReceivers.keySet()) {
         log.debugf("Processing entry %s", scp);
         if (scp.localCacheName.equals(cse.getCacheName())) {
            log.debugf("Deregistering backup receiver %s", scp);
            backupReceivers.remove(scp);
         }
      }
   }

   @Override
   public Object handleRemoteCommand(SingleRpcCommand cmd, SiteAddress src) throws Throwable {
      log.tracef("Handling command %s from remote site %s", cmd, src);
      String name = cmd.getCacheName();
      BackupReceiver localBackupCache = getBackupCacheManager(SiteUUID.getSiteName(src.getSite()), name);
      return localBackupCache.handleRemoteCommand((VisitableCommand)cmd.getCommand());
   }

   /**
    * Returns the local cache associated defined as backup for the provided remote (site, cache) combo, or throws an
    * exception of no such site is defined.
    * <p/>
    * Also starts the cache if not already stated; that is because the cache is needed for update after when this method
    * is invoked.
    */
   public BackupReceiver getBackupCacheManager(String remoteSite, String remoteCache) {
      SiteCachePair toLookFor = new SiteCachePair(remoteCache, remoteSite);
      BackupReceiver backupManager = backupReceivers.get(toLookFor);
      if (backupManager != null) return backupManager;

      //check the default cache first
      Configuration dcc = cacheManager.getDefaultCacheConfiguration();
      if (isBackupForRemoteCache(remoteSite, remoteCache, dcc, EmbeddedCacheManager.DEFAULT_CACHE_NAME)) {
         Cache<Object, Object> cache = cacheManager.getCache();
         backupReceivers.putIfAbsent(toLookFor, new BackupReceiverImpl(cache));
         toLookFor.setLocalCacheName(EmbeddedCacheManager.DEFAULT_CACHE_NAME);
         return backupReceivers.get(toLookFor);
      }

      Set<String> cacheNames = cacheManager.getCacheNames();
      for (String name : cacheNames) {
         Configuration cacheConfiguration = cacheManager.getCacheConfiguration(name);
         if (isBackupForRemoteCache(remoteSite, remoteCache, cacheConfiguration, name)) {
            Cache<Object, Object> cache = cacheManager.getCache(name);
            toLookFor.setLocalCacheName(name);
            backupReceivers.putIfAbsent(toLookFor, new BackupReceiverImpl(cache));
            return backupReceivers.get(toLookFor);
         }
      }
      log.debugf("Did not find any backup explicitly configured backup cache for remote cache/site: %s/%s. Using %s",
                 remoteSite, remoteCache, remoteCache);

      Cache<Object, Object> cache = cacheManager.getCache(remoteCache);
      backupReceivers.putIfAbsent(toLookFor, new BackupReceiverImpl(cache));
      toLookFor.setLocalCacheName(cache.getName());
      return backupReceivers.get(toLookFor);
   }

   private boolean isBackupForRemoteCache(String remoteSite, String remoteCache, Configuration cacheConfiguration, String name) {
      boolean found = cacheConfiguration.sites().backupFor().isBackupFor(remoteSite, remoteCache);
      if (found)
         log.tracef("Found local cache '%s' is backup for cache '%s' from site '%s'", name, remoteCache, remoteSite);
      return found;
   }

   static class SiteCachePair {
      public final String remoteSite;
      public final String remoteCache;
      public String localCacheName;

      /**
       * Important: do not include the localCacheName field in the equals and hash code comparison. This is mainly used
       * as a key in a map and the localCacheName field might change causing troubles.
       */
      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof SiteCachePair)) return false;

         SiteCachePair that = (SiteCachePair) o;

         if (remoteCache != null ? !remoteCache.equals(that.remoteCache) : that.remoteCache != null) return false;
         if (remoteSite != null ? !remoteSite.equals(that.remoteSite) : that.remoteSite != null) return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = remoteSite != null ? remoteSite.hashCode() : 0;
         result = 31 * result + (remoteCache != null ? remoteCache.hashCode() : 0);
         return result;
      }

      SiteCachePair(String remoteCache, String remoteSite) {
         this.remoteCache = remoteCache;
         this.remoteSite = remoteSite;
      }

      public void setLocalCacheName(String localCacheName) {
         this.localCacheName = localCacheName;
      }

      @Override
      public String toString() {
         return "SiteCachePair{" +
               "site='" + remoteSite + '\'' +
               ", cache='" + remoteCache + '\'' +
               '}';
      }
   }

   public void replace(String site, String cache, BackupReceiver bcr) {
      backupReceivers.replace(new SiteCachePair(cache, site), bcr);
   }

   public BackupReceiver getBackupReceiver(String site, String cache) {
      return backupReceivers.get(new SiteCachePair(site, cache));
   }
}
