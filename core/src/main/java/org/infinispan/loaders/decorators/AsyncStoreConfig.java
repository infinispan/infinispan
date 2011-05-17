/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.loaders.decorators;

import org.infinispan.config.AbstractNamedCacheConfigurationBean;
import org.infinispan.config.ConfigurationBeanVisitor;
import org.infinispan.config.ConfigurationDoc;
import org.infinispan.config.ConfigurationDocRef;
import org.infinispan.config.Dynamic;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

/**
 * Configuration for the async cache loader. If enabled, this provides you with asynchronous writes
 * to the cache store, giving you 'write-behind' caching.
 * 
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @since 4.0
 * 
 * @see <a href="../../../../config.html#ce_loader_async">Configuration reference</a>
 */
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
@ConfigurationDoc(name="async",parentName="loader")
public class AsyncStoreConfig extends AbstractNamedCacheConfigurationBean {

   /** The serialVersionUID */
   private static final long serialVersionUID = -8596800049019004961L;

   @ConfigurationDocRef(bean=AsyncStoreConfig.class,targetElement="setEnabled")
   protected Boolean enabled = false;

   @ConfigurationDocRef(bean=AsyncStoreConfig.class,targetElement="setThreadPoolSize")
   protected Integer threadPoolSize = 1;

   @Dynamic
   @ConfigurationDocRef(bean=AsyncStoreConfig.class,targetElement="setFlushLockTimeout")
   protected Long flushLockTimeout = 5000L;

   // By default a bit under the default cache stop timeout
   @Dynamic
   @ConfigurationDocRef(bean=AsyncStoreConfig.class,targetElement="setShutdownTimeout")
   protected Long shutdownTimeout = 25000L;

   @XmlAttribute
   public Boolean isEnabled() {
      return enabled;
   }

   /**
    * If true, all modifications to this cache store happen asynchronously, on a separate thread.
    * 
    * @param enabled
    */
   public void setEnabled(Boolean enabled) {
      testImmutability("enabled");
      this.enabled = enabled;
   }

   @XmlAttribute
   public Integer getThreadPoolSize() {
      return threadPoolSize;
   }

   /**
    * Size of the thread pool whose threads are responsible for applying the modifications.
    * 
    * @param threadPoolSize
    */
   public void setThreadPoolSize(Integer threadPoolSize) {
      testImmutability("threadPoolSize");
      this.threadPoolSize = threadPoolSize;
   }

   @XmlAttribute
   public Long getFlushLockTimeout() {
      return flushLockTimeout;
   }

   /**
    * Timeout to acquire the lock which guards the state to be flushed to the cache store
    * periodically.
    * 
    * @param stateLockTimeout
    */
   public void setFlushLockTimeout(Long stateLockTimeout) {
      testImmutability("flushLockTimeout");
      this.flushLockTimeout = stateLockTimeout;
   }

   @XmlAttribute
   public Long getShutdownTimeout() {
      return shutdownTimeout;
   }

   /**
    * Timeout to stop the cache store. When the store is stopped it's possible that some
    * modifications still need to be applied; you likely want to set a very large timeout to make
    * sure to not loose data
    * 
    * @param shutdownTimeout
    */
   public void setShutdownTimeout(Long shutdownTimeout) {
      testImmutability("shutdownTimeout");
      this.shutdownTimeout = shutdownTimeout;
   }

   @Override
   public AsyncStoreConfig clone() {
      try {
         return (AsyncStoreConfig) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new RuntimeException("Should not happen!", e);
      }
   }

   public void accept(ConfigurationBeanVisitor v) {
      v.visitAsyncStoreConfig(this);
   }
}
