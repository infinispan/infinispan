/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.util.Util;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import java.util.LinkedList;
import java.util.List;

/**
 * Holds the configuration of the cache loader chain. ALL cache loaders should be defined using this
 * class, adding individual cache loaders to the chain by calling
 * {@link CacheLoaderManagerConfig#addCacheLoaderConfig}
 * 
 * <p>
 * Note that class CacheLoaderManagerConfig contains JAXB annotations. These annotations determine
 * how XML configuration files are read into instances of configuration class hierarchy as well as
 * they provide meta data for configuration file XML schema generation. Please modify these
 * annotations and Java element types they annotate with utmost understanding and care.
 * 
 * @configRef name="loaders",desc="Holds the configuration of the cache loader chain."
 * 
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author Brian Stansberry
 * @author Vladimir Blagojevic
 * @author <a href="mailto:galder.zamarreno@jboss.com">Galder Zamarreno</a>
 * @since 4.0
 */
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class CacheLoaderManagerConfig extends AbstractNamedCacheConfigurationBean {
   
   private static final long serialVersionUID = 2210349340378984424L;

   /**
    * @configRef desc="If true, data is only written to the cache store when it is evicted from memory, 
    *            a phenomenon known as passivation. Next time the data is requested, it will be activated which
    *            means that data will be brought back to memory and deleted from the persistent store. 
    *            If false, the cache store contains a copy of the contents in memory, so writes to cache 
    *            result in cache store writes."
    * */
   protected Boolean passivation = false;

   /**
    * @configRef desc= "If true, when the cache starts, data stored in the cache store will be pre-loaded into 
    *            memory. This is particularly useful when data in the cache store will be needed immediately 
    *            after startup and you want to avoid cache operations being delayed as a result of loading this
    *            data."
    * */
   protected Boolean preload = false;

   /**
    * @configRef desc="This setting should be set to true when multiple cache instances share the same cache store 
    *            in order to avoid multiple cache writing the same cache modification multiple times. So if true, 
    *            only the node where the cache modification originated will write to the cache store. If false, 
    *            each individual cache reacts to a potential replication message by storing the data to the cache 
    *            store. Note that this could be useful if each individual cache has its own cache store.
    * */
   protected Boolean shared = false;

   protected List<CacheLoaderConfig> cacheLoaderConfigs = new LinkedList<CacheLoaderConfig>();

   public Boolean isPreload() {
      return preload;
   }

   @XmlAttribute
   public void setPreload(Boolean preload) {
      testImmutability("preload");
      this.preload = preload;
   }

   @XmlAttribute
   public void setPassivation(Boolean passivation) {
      testImmutability("passivation");
      this.passivation = passivation;
   }

   public Boolean isPassivation() {
      return passivation;
   }

   @XmlAttribute
   public void setShared(Boolean shared) {
      testImmutability("shared");
      this.shared = shared;
   }

   public Boolean isShared() {
      return shared;
   }

   public void addCacheLoaderConfig(CacheLoaderConfig clc) {
      testImmutability("cacheLoaderConfigs");
      cacheLoaderConfigs.add(clc);
   }

   @XmlElement(name = "loader")
   public List<CacheLoaderConfig> getCacheLoaderConfigs() {
      testImmutability("cacheLoaderConfigs");
      return cacheLoaderConfigs;
   }

   public void setCacheLoaderConfigs(List<CacheLoaderConfig> configs) {
      testImmutability("cacheLoaderConfigs");
      this.cacheLoaderConfigs = configs == null ? new LinkedList<CacheLoaderConfig>() : configs;
   }

   public CacheLoaderConfig getFirstCacheLoaderConfig() {
      if (cacheLoaderConfigs.size() == 0)
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

   public boolean useChainingCacheLoader() {
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
