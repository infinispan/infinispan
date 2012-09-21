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
package org.infinispan.configuration.global;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.Version;

import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * <p>
 * Configuration component that exposes the global configuration.
 * </p>
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @author Pete Muir
 * @since 5.1
 *
 * @see <a href="../../../config.html#ce_infinispan_global">Configuration reference</a>
 *
 */
@Scope(Scopes.GLOBAL)
@SurvivesRestarts
public class GlobalConfiguration {

   /**
    * Default replication version, from {@link org.infinispan.Version#getVersionShort}.
    */
   public static final short DEFAULT_MARSHALL_VERSION = Version.getVersionShort();

   private final ExecutorFactoryConfiguration asyncListenerExecutor;
   private final ExecutorFactoryConfiguration asyncTransportExecutor;
   private final ScheduledExecutorFactoryConfiguration evictionScheduledExecutor;
   private final ScheduledExecutorFactoryConfiguration replicationQueueScheduledExecutor;
   private final GlobalJmxStatisticsConfiguration globalJmxStatistics;
   private final TransportConfiguration transport;
   private final SerializationConfiguration serialization;
   private final ShutdownConfiguration shutdown;
   private final Map<Class<?>, ?> modules;
   private final SitesConfiguration sites;
   private final ClassLoader cl;

   GlobalConfiguration(ExecutorFactoryConfiguration asyncListenerExecutor,
         ExecutorFactoryConfiguration asyncTransportExecutor, ScheduledExecutorFactoryConfiguration evictionScheduledExecutor,
         ScheduledExecutorFactoryConfiguration replicationQueueScheduledExecutor, GlobalJmxStatisticsConfiguration globalJmxStatistics,
         TransportConfiguration transport, SerializationConfiguration serialization, ShutdownConfiguration shutdown,
         List<?> modules, SitesConfiguration sites ,ClassLoader cl) {
      this.asyncListenerExecutor = asyncListenerExecutor;
      this.asyncTransportExecutor = asyncTransportExecutor;
      this.evictionScheduledExecutor = evictionScheduledExecutor;
      this.replicationQueueScheduledExecutor = replicationQueueScheduledExecutor;
      this.globalJmxStatistics = globalJmxStatistics;
      this.transport = transport;
      this.serialization = serialization;
      this.shutdown = shutdown;
      Map<Class<?>, Object> moduleMap = new HashMap<Class<?>, Object>();
      for(Object module : modules) {
         moduleMap.put(module.getClass(), module);
      }
      this.modules = Collections.unmodifiableMap(moduleMap);
      this.sites = sites;
      this.cl = cl;
   }

   public ExecutorFactoryConfiguration asyncListenerExecutor() {
      return asyncListenerExecutor;
   }

   public ExecutorFactoryConfiguration asyncTransportExecutor() {
      return asyncTransportExecutor;
   }

   public ScheduledExecutorFactoryConfiguration evictionScheduledExecutor() {
      return evictionScheduledExecutor;
   }

   public ScheduledExecutorFactoryConfiguration replicationQueueScheduledExecutor() {
      return replicationQueueScheduledExecutor;
   }

   public GlobalJmxStatisticsConfiguration globalJmxStatistics() {
      return globalJmxStatistics;
   }

   public TransportConfiguration transport() {
      return transport;
   }

   public SerializationConfiguration serialization() {
      return serialization;
   }

   public ShutdownConfiguration shutdown() {
      return shutdown;
   }

   @SuppressWarnings("unchecked")
   public <T> T module(Class<T> moduleClass) {
      return (T)modules.get(moduleClass);
   }

   public Map<Class<?>, ?> modules() {
      return modules;
   }

   /**
    * Get the classloader in use by this configuration.
    */
   public ClassLoader classLoader() {
      return cl;
   }

   public SitesConfiguration sites() {
      return sites;
   }
   
   @Override
   public String toString() {
      return "GlobalConfiguration{" +
            "asyncListenerExecutor=" + asyncListenerExecutor +
            ", asyncTransportExecutor=" + asyncTransportExecutor +
            ", evictionScheduledExecutor=" + evictionScheduledExecutor +
            ", replicationQueueScheduledExecutor=" + replicationQueueScheduledExecutor +
            ", globalJmxStatistics=" + globalJmxStatistics +
            ", transport=" + transport +
            ", serialization=" + serialization +
            ", shutdown=" + shutdown +
            ", modules=" + modules +
            ", sites=" + sites +
            ", cl=" + cl +
            '}';
   }
}
