/**
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
 *   ~
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

package org.infinispan.spring;

import static org.infinispan.client.hotrod.impl.ConfigurationProperties.ASYNC_EXECUTOR_FACTORY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.FORCE_RETURN_VALUES;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.KEY_SIZE_ESTIMATE;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.MARSHALLER;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.PING_ON_STARTUP;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.REQUEST_BALANCING_STRATEGY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SERVER_LIST;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TCP_NO_DELAY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TRANSPORT_FACTORY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.VALUE_SIZE_ESTIMATE;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

/**
 * <p>
 * Provides a mechanism to override selected configuration properties using explicit setters for
 * each configuration setting.
 * </p>
 * 
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * 
 */
public class ConfigurationPropertiesOverrides {

   private final Properties overridingProperties = new Properties();

   /**
    * @return
    * @see java.util.Hashtable#isEmpty()
    */
   public boolean isEmpty() {
      return this.overridingProperties.isEmpty();
   }

   /**
    * @param TransportFactory
    */
   public void setTransportFactory(final String TransportFactory) {
      this.overridingProperties.setProperty(TRANSPORT_FACTORY, TransportFactory);
   }

   /**
    * @param serverList
    */
   public void setServerList(final Collection<InetSocketAddress> serverList) {
      final StringBuilder serverListStr = new StringBuilder();
      for (final InetSocketAddress server : serverList) {
         serverListStr.append(server.getHostName()).append(":").append(server.getPort())
                  .append(";");
      }
      serverListStr.deleteCharAt(serverListStr.length() - 1);
      this.overridingProperties.setProperty(SERVER_LIST, serverListStr.toString());
   }

   /**
    * @param marshaller
    */
   public void setMarshaller(final String marshaller) {
      this.overridingProperties.setProperty(MARSHALLER, marshaller);
   }

   /**
    * @param asyncExecutorFactory
    */
   public void setAsyncExecutorFactory(final String asyncExecutorFactory) {
      this.overridingProperties.setProperty(ASYNC_EXECUTOR_FACTORY, asyncExecutorFactory);
   }

   /**
    * @param tcpNoDelay
    */
   public void setTcpNoDelay(final boolean tcpNoDelay) {
      this.overridingProperties.setProperty(TCP_NO_DELAY, Boolean.toString(tcpNoDelay));
   }

   /**
    * @param pingOnStartup
    */
   public void setPingOnStartup(final boolean pingOnStartup) {
      this.overridingProperties.setProperty(PING_ON_STARTUP, Boolean.toString(pingOnStartup));
   }

   /**
    * @param requestBalancingStrategy
    */
   public void setRequestBalancingStrategy(final String requestBalancingStrategy) {
      this.overridingProperties.setProperty(REQUEST_BALANCING_STRATEGY, requestBalancingStrategy);
   }

   /**
    * @param keySizeEstimate
    */
   public void setKeySizeEstimate(final int keySizeEstimate) {
      this.overridingProperties.setProperty(KEY_SIZE_ESTIMATE, Integer.toString(keySizeEstimate));
   }

   /**
    * @param valueSizeEstimate
    */
   public void setValueSizeEstimate(final int valueSizeEstimate) {
      this.overridingProperties.setProperty(VALUE_SIZE_ESTIMATE,
               Integer.toString(valueSizeEstimate));
   }

   /**
    * @param forceReturnValues
    */
   public void setForceReturnValues(final boolean forceReturnValues) {
      this.overridingProperties.setProperty(FORCE_RETURN_VALUES,
               Boolean.toString(forceReturnValues));
   }

   /**
    * @param configurationPropertiesToOverride
    * @return
    */
   public Properties override(final Properties configurationPropertiesToOverride) {
      final Properties answer = Properties.class.cast(configurationPropertiesToOverride.clone());
      for (final Map.Entry<Object, Object> prop : this.overridingProperties.entrySet()) {
         answer.setProperty(String.class.cast(prop.getKey()), String.class.cast(prop.getValue()));
      }
      return answer;
   }
}
