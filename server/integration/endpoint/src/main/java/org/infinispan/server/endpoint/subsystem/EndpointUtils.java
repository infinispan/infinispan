/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
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
package org.infinispan.server.endpoint.subsystem;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.endpoint.Constants;
import org.jboss.as.clustering.infinispan.subsystem.EmbeddedCacheManagerConfiguration;
import org.jboss.as.clustering.infinispan.subsystem.EmbeddedCacheManagerConfigurationService;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.PathElement;
import org.jboss.as.core.security.ServerSecurityManager;
import org.jboss.as.domain.management.SecurityRealm;
import org.jboss.as.domain.management.SecurityRealm.ServiceUtil;
import org.jboss.as.network.SocketBinding;
import org.jboss.as.security.plugins.SecurityDomainContext;
import org.jboss.as.security.service.SecurityDomainService;
import org.jboss.as.security.service.SimpleSecurityManagerService;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.value.InjectedValue;


public class EndpointUtils {
   private static final String INFINISPAN_SERVICE_NAME = "infinispan";

   public static ServiceName getCacheServiceName(String cacheContainerName, String cacheName) {
      ServiceName cacheServiceName = getCacheContainerServiceName(cacheContainerName);
      if (cacheName != null) {
         cacheServiceName = cacheServiceName.append(cacheName);
      } else {
         cacheServiceName = cacheServiceName.append(CacheContainer.DEFAULT_CACHE_NAME);
      }
      return cacheServiceName;
   }

   public static ServiceName getCacheContainerServiceName(String cacheContainerName) {
      ServiceName cacheContainerServiceName = ServiceName.JBOSS.append(INFINISPAN_SERVICE_NAME);
      if (cacheContainerName != null) {
         cacheContainerServiceName = cacheContainerServiceName.append(cacheContainerName);
      }
      return cacheContainerServiceName;
   }

   public static ServiceName getServiceName(final ModelNode node, final String... prefix) {
      final PathAddress address = PathAddress.pathAddress(node.require(OP_ADDR));
      final String name = address.getLastElement().getValue();
      if (prefix.length > 0) {
         return Constants.DATAGRID.append(prefix).append(name);
      } else {
         return Constants.DATAGRID.append(name);
      }
   }

   public static void addCacheDependency(ServiceBuilder<?> builder, String cacheContainerName, String cacheName) {
      ServiceName cacheServiceName = getCacheServiceName(cacheContainerName, cacheName);
      builder.addDependency(cacheServiceName);
   }

   public static void addCacheContainerConfigurationDependency(ServiceBuilder<?> builder, String cacheContainerName,
         InjectedValue<EmbeddedCacheManagerConfiguration> target) {
      ServiceName cacheContainerConfigurationServiceName = EmbeddedCacheManagerConfigurationService
            .getServiceName(cacheContainerName);
      builder.addDependency(cacheContainerConfigurationServiceName, EmbeddedCacheManagerConfiguration.class, target);
   }

   public static void addCacheContainerDependency(ServiceBuilder<?> builder, String cacheContainerName, InjectedValue<EmbeddedCacheManager> target) {
      ServiceName cacheContainerServiceName = getCacheContainerServiceName(cacheContainerName);
      builder.addDependency(cacheContainerServiceName, EmbeddedCacheManager.class, target);
   }

   public static void addSocketBindingDependency(ServiceBuilder<?> builder, String socketBindingName, InjectedValue<SocketBinding> target) {
      ServiceName socketName = SocketBinding.JBOSS_BINDING_NAME.append(socketBindingName);
      builder.addDependency(socketName, SocketBinding.class, target);
   }

   public static void addSecurityDomainDependency(ServiceBuilder<?> builder, String securityDomainName, InjectedValue<SecurityDomainContext> target) {
      ServiceName securityDomainServiceName = SecurityDomainService.SERVICE_NAME.append(securityDomainName);
      builder.addDependency(securityDomainServiceName, SecurityDomainContext.class, target);
   }

   public static void addSecurityRealmDependency(ServiceBuilder<?> builder, String securityRealmName, InjectedValue<SecurityRealm> target) {
      ServiceName securityRealmServiceName = ServiceUtil.createServiceName(securityRealmName);
      builder.addDependency(securityRealmServiceName, SecurityRealm.class, target);
   }

   public static ModelNode pathAddress(PathElement... elements) {
      return PathAddress.pathAddress(elements).toModelNode();
   }

   public static void copyIfSet(String name, ModelNode source, ModelNode target) {
      if (source.hasDefined(name)) {
         target.get(name).set(source.get(name));
      }
   }

   public static void addServerSecurityManagerDependency(ServiceBuilder<?> builder, InjectedValue<ServerSecurityManager> serverSecurityManager) {
      builder.addDependency(SimpleSecurityManagerService.SERVICE_NAME, ServerSecurityManager.class, serverSecurityManager);
   }
}
