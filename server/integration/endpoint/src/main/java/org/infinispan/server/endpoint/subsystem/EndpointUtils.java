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

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.RestServer;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.infinispan.spi.service.CacheContainerServiceName;
import org.infinispan.server.infinispan.spi.service.CacheServiceName;
import org.jboss.as.controller.OperationContext;
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

import static org.infinispan.server.endpoint.Constants.DATAGRID;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;


public class EndpointUtils {
   public static ServiceName getCacheServiceName(String cacheContainerName, String cacheName) {
      if (cacheName != null) {
         return CacheServiceName.CACHE.getServiceName(cacheContainerName, cacheName);
      } else {
         return CacheServiceName.CACHE.getServiceName(cacheContainerName);
      }
   }

   public static ServiceName getCacheContainerServiceName(String cacheContainerName) {
      return CacheContainerServiceName.CACHE_CONTAINER.getServiceName(cacheContainerName);
   }

   public static ServiceName getServiceName(final ModelNode node, final String... prefix) {
      final PathAddress address = PathAddress.pathAddress(node.require(OP_ADDR));
      final String name = address.getLastElement().getValue();
      if (prefix.length > 0) {
         return DATAGRID.append(prefix).append(name);
      } else {
         return DATAGRID.append(name);
      }
   }

   public static void addCacheDependency(ServiceBuilder<?> builder, String cacheContainerName, String cacheName) {
      ServiceName cacheServiceName = getCacheServiceName(cacheContainerName, cacheName);
      builder.addDependency(cacheServiceName);
   }

   public static void addCacheContainerConfigurationDependency(ServiceBuilder<?> builder, String cacheContainerName,
         InjectedValue<GlobalConfiguration> target) {
      ServiceName cacheContainerConfigurationServiceName = CacheContainerServiceName.CONFIGURATION
            .getServiceName(cacheContainerName);
      builder.addDependency(cacheContainerConfigurationServiceName, GlobalConfiguration.class, target);
   }

   public static void addCacheContainerDependency(ServiceBuilder<?> builder, String cacheContainerName, InjectedValue<EmbeddedCacheManager> target) {
      ServiceName cacheContainerServiceName = getCacheContainerServiceName(cacheContainerName);
      builder.addDependency(cacheContainerServiceName, EmbeddedCacheManager.class, target);
   }

   public static void addHotRodDependency(ServiceBuilder<?> builder, String protocolServerName, InjectedValue<HotRodServer> target) {
      ServiceName protocolServerServiceName = DATAGRID.append("hotrod").append(protocolServerName);
      builder.addDependency(protocolServerServiceName, HotRodServer.class, target);
   }

   public static void addRestDependency(ServiceBuilder<?> builder, String protocolServerName, InjectedValue<RestServer> target) {
      ServiceName protocolServerServiceName = DATAGRID.append("rest").append(protocolServerName);
      builder.addDependency(protocolServerServiceName, RestServer.class, target);
   }

   public static void addSocketBindingDependency(OperationContext context, ServiceBuilder<?> builder, String socketBindingName,
                                                              InjectedValue<SocketBinding> target) {
      // socket binding can be disabled in multi tenant router scenarios
      if(socketBindingName != null) {
         ServiceName serviceName = context.getCapabilityServiceName(ProtocolServerConnectorResource.SOCKET_CAPABILITY_NAME, socketBindingName, SocketBinding.class);
         builder.addDependency(serviceName, SocketBinding.class, target);
      }
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
