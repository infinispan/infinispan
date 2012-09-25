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
package org.infinispan.cdi.test.testutil;

import org.infinispan.cdi.ConfigureCache;
import org.infinispan.cdi.event.AbstractEventBridge;
import org.infinispan.cdi.event.cache.CacheEventBridge;
import org.infinispan.cdi.event.cachemanager.CacheManagerEventBridge;
import org.infinispan.cdi.interceptor.CacheResultInterceptor;
import org.infinispan.cdi.interceptor.context.CacheKeyInvocationContextFactory;
import org.infinispan.cdi.interceptor.context.metadata.MethodMetaData;
import org.infinispan.cdi.util.CacheLookupHelper;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.DependencyResolvers;
import org.jboss.shrinkwrap.resolver.api.maven.MavenDependencyResolver;

import java.io.File;

/**
 * Arquillian deployment utility class.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public final class Deployments {
   /**
    * The base deployment web archive. The CDI extension is packaged as an individual jar.
    */
   public static WebArchive baseDeployment() {
      String ideFriendlyPath = "cdi/extension/pom.xml";
      // Figure out an IDE and Maven friendly path:
      String pomPath = new File(ideFriendlyPath).getAbsoluteFile().exists() ? ideFriendlyPath : "pom.xml";

      return ShrinkWrap.create(WebArchive.class, "test.war")
            .addAsWebInfResource(Deployments.class.getResource("/beans.xml"), "beans.xml")
            .addAsLibrary(
                  ShrinkWrap.create(JavaArchive.class, "infinispan-cdi.jar")
                        .addPackage(ConfigureCache.class.getPackage())
                        .addPackage(AbstractEventBridge.class.getPackage())
                        .addPackage(CacheEventBridge.class.getPackage())
                        .addPackage(CacheManagerEventBridge.class.getPackage())
                        .addPackage(CacheResultInterceptor.class.getPackage())
                        .addPackage(CacheLookupHelper.class.getPackage())
                        .addPackage(CacheKeyInvocationContextFactory.class.getPackage())
                        .addPackage(MethodMetaData.class.getPackage())
                        .addAsManifestResource(ConfigureCache.class.getResource("/META-INF/beans.xml"), "beans.xml")
                        .addAsManifestResource(ConfigureCache.class.getResource("/META-INF/services/javax.enterprise.inject.spi.Extension"), "services/javax.enterprise.inject.spi.Extension")
            )
            .addAsLibraries(
                  DependencyResolvers.use(MavenDependencyResolver.class)
                        .loadMetadataFromPom(pomPath)
                        .artifact("org.jboss.solder:solder-impl")
                        .resolveAs(JavaArchive.class)
            );
   }
}
