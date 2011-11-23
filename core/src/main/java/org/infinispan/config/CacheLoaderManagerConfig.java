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
package org.infinispan.config;

import java.util.LinkedList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlTransient;

import org.infinispan.commons.util.Util;
import org.infinispan.config.FluentConfiguration.LoadersConfig;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheStoreConfig;

/**
 * Holds the configuration of the cache loader chain. All cache loaders should be defined using this
 * class, adding individual cache loaders to the chain by calling
 * {@link CacheLoaderManagerConfig#addCacheLoaderConfig}
 * 
 *
 * 
 * @see <a href="../../../config.html#ce_default_loaders">Configuration reference</a>
 * 
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author Brian Stansberry
 * @author Vladimir Blagojevic
 * @author <a href="mailto:galder.zamarreno@jboss.com">Galder Zamarreno</a>
 * @since 4.0
 */
@XmlAccessorType(XmlAccessType.PROPERTY)
@ConfigurationDoc(name="loaders",desc="Holds the configuration for cache loaders and stores")
public class CacheLoaderManagerConfig extends AbstractFluentConfigurationBean implements LoadersConfig {

   private static final long serialVersionUID = 2210349340378984424L;

   @ConfigurationDocRef(bean=CacheLoaderManagerConfig.class,targetElement="setPassivation")
   protected Boolean passivation = false;

   @ConfigurationDocRef(bean=CacheLoaderManagerConfig.class,targetElement="setPreload")   
   protected Boolean preload = false;

   @ConfigurationDocRef(bean=CacheLoaderManagerConfig.class,targetElement="setShared")   
   protected Boolean shared = false;
  
   protected List<CacheLoaderConfig> cacheLoaderConfigs = new LinkedList<CacheLoaderConfig>();

   public CacheLoaderManagerConfig() {
   }

   public CacheLoaderManagerConfig(CacheLoaderConfig clc) {
      addCacheLoaderConfig(clc);
   }

   public Boolean isPreload() {
      return preload;
   }

   /**
    * If true, when the cache starts, data stored in the cache store will be pre-loaded into memory.
    * This is particularly useful when data in the cache store will be needed immediately after
    * startup and you want to avoid cache operations being delayed as a result of loading this data
    * lazily. Can be used to provide a 'warm-cache' on startup, however there is a performance
    * penalty as startup time is affected by this process.
    * 
    * @param preload
    */
   public LoadersConfig preload(Boolean preload) {
      testImmutability("preload");
      this.preload = preload;
      return this;
   }

   /**
    * @deprecated The visibility of this method will be reduced. Use {@link #preload(Boolean)} instead.
    */
   @XmlAttribute
   @Deprecated
   public void setPreload(Boolean preload) {
      testImmutability("preload");
      this.preload = preload;
   }

   /**
    * If true, data is only written to the cache store when it is evicted from memory, a phenomenon
    * known as 'passivation'. Next time the data is requested, it will be 'activated' which means
    * that data will be brought back to memory and removed from the persistent store. This gives you
    * the ability to 'overflow' to disk, similar to swapping in an operating system. <br />
    * <br />
    * If false, the cache store contains a copy of the contents in memory, so writes to cache result
    * in cache store writes. This essentially gives you a 'write-through' configuration.
    * 
    * @param passivation
    */   
   @Override
   public LoadersConfig passivation(Boolean passivation) {
      testImmutability("passivation");
      this.passivation = passivation;
      return this;
   }
   
   /**
    * @deprecated The visibility of this method will be reduced. Use {@link #passivation(Boolean)} instead.
    */
   @XmlAttribute
   @Deprecated
   public void setPassivation(Boolean passivation) {
      testImmutability("passivation");
      this.passivation = passivation;
   }

   public Boolean isPassivation() {
      return passivation;
   }

   /**
    * This setting should be set to true when multiple cache instances share the same cache store
    * (e.g., multiple nodes in a cluster using a JDBC-based CacheStore pointing to the same, shared
    * database.) Setting this to true avoids multiple cache instances writing the same modification
    * multiple times. If enabled, only the node where the modification originated will write to the
    * cache store. <br />
    * <br />
    * If disabled, each individual cache reacts to a potential remote update by storing the data to
    * the cache store. Note that this could be useful if each individual node has its own cache
    * store - perhaps local on-disk.
    * 
    * @param shared
    */   
   @Override
   public LoadersConfig shared(Boolean shared) {
      testImmutability("shared");
      this.shared = shared;
      return this;
   }

   @Override
   public LoadersConfig addCacheLoader(CacheLoaderConfig... configs) {
      for (CacheLoaderConfig config : configs)
         addCacheLoaderConfig(config);
      return this;
   }

   /**
    * @deprecated The visibility of this method will be reduced. Use {@link #shared(Boolean)} instead.
    */
   @XmlAttribute
   @Deprecated
   public void setShared(Boolean shared) {
      testImmutability("shared");
      this.shared = shared;
   }

   public Boolean isShared() {
      return shared;
   }

   /**
    *
    * @param clc
    * @return
    * @deprecated use {@link #addCacheLoader(org.infinispan.loaders.CacheLoaderConfig...)} instead
    */
   @Deprecated
   public LoadersConfig addCacheLoaderConfig(CacheLoaderConfig clc) {
      testImmutability("cacheLoaderConfigs");
      cacheLoaderConfigs.add(clc);
      return this;
   }
   
   public List<CacheLoaderConfig> getCacheLoaderConfigs() {
      return cacheLoaderConfigs;
   }

   // JAXB method
   private List<CacheLoaderConfig> getCacheLoaders() {
      testImmutability("cacheLoaderConfigs");
      return cacheLoaderConfigs;
   }

   // JAXB method
   @XmlElement(name = "loader")
   private LoadersConfig setCacheLoaders(List<CacheLoaderConfig> configs) {
      testImmutability("cacheLoaderConfigs");
      this.cacheLoaderConfigs = configs == null ? new LinkedList<CacheLoaderConfig>() : configs;
      return this;
   }

   /**
    * @deprecated The visibility of this method will be reduced and
    * XMLElement definition is likely to move to the getCacheLoaderConfigs().
    */
   @Deprecated
   @XmlTransient // Avoid JAXB finding this method
   public LoadersConfig setCacheLoaderConfigs(List<CacheLoaderConfig> configs) {
      testImmutability("cacheLoaderConfigs");
      this.cacheLoaderConfigs = configs == null ? new LinkedList<CacheLoaderConfig>() : configs;
      return this;
   }

   public CacheLoaderConfig getFirstCacheLoaderConfig() {
      if (cacheLoaderConfigs.isEmpty())
         return null;
      return cacheLoaderConfigs.get(0);
   }

   /**
    * Loops through all individual cache loader configs and checks if fetchPersistentState is set on
    * any of them
    */
   public Boolean isFetchPersistentState() {
      for (CacheLoaderConfig iclc : cacheLoaderConfigs) {
         if (iclc instanceof CacheStoreConfig)
            if (((CacheStoreConfig) iclc).isFetchPersistentState())
               return true;
      }
      return false;
   }

   @Override
   protected CacheLoaderManagerConfig setConfiguration(Configuration config) {
      super.setConfiguration(config);
      return this;
   }

   public boolean usingChainingCacheLoader() {
      return !isPassivation() && cacheLoaderConfigs.size() > 1;
   }

   @Override
   public String toString() {
      return new StringBuilder().append("CacheLoaderManagerConfig{").append("shared=").append(
               shared).append(", passivation=").append(passivation).append(", preload='").append(
               preload).append('\'').append(", cacheLoaderConfigs.size()=").append(
               cacheLoaderConfigs.size()).append('}').toString();
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;

      if (obj instanceof CacheLoaderManagerConfig) {
         CacheLoaderManagerConfig other = (CacheLoaderManagerConfig) obj;
         return (this.passivation.equals(other.passivation)) && (this.shared.equals(other.shared))
                  && Util.safeEquals(this.preload, other.preload)
                  && Util.safeEquals(this.cacheLoaderConfigs, other.cacheLoaderConfigs);
      }
      return false;
   }

   public void accept(ConfigurationBeanVisitor v) {
      for (CacheLoaderConfig clc : cacheLoaderConfigs) {
         clc.accept(v);
      }
      v.visitCacheLoaderManagerConfig(this);
   }

   @Override
   public int hashCode() {
      int result = 19;
      result = 51 * result + (passivation ? 0 : 1);
      result = 51 * result + (shared ? 0 : 1);
      result = 51 * result + (preload ? 0 : 1);
      result = 51 * result + (cacheLoaderConfigs == null ? 0 : cacheLoaderConfigs.hashCode());
      return result;
   }

   @Override
   public CacheLoaderManagerConfig clone() throws CloneNotSupportedException {
      CacheLoaderManagerConfig clone = (CacheLoaderManagerConfig) super.clone();
      if (cacheLoaderConfigs != null) {
         List<CacheLoaderConfig> clcs = new LinkedList<CacheLoaderConfig>();
         for (CacheLoaderConfig clc : cacheLoaderConfigs) {
            clcs.add(clc.clone());
         }
         clone.cacheLoaderConfigs = clcs;
      }
      return clone;
   }
}
