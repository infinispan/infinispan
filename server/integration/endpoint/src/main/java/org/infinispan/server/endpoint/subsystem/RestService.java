/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011-2013 Red Hat Inc. and/or its affiliates and other contributors
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

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.resource.FileResourceManager;
import io.undertow.servlet.api.Deployment;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.servlet.api.ListenerInfo;
import io.undertow.servlet.api.LoginConfig;
import io.undertow.servlet.api.MimeMapping;
import io.undertow.servlet.api.SecurityConstraint;
import io.undertow.servlet.api.ServletInfo;
import io.undertow.servlet.api.WebResourceCollection;
import io.undertow.servlet.handlers.DefaultServlet;

import java.io.File;

import javax.servlet.ServletContext;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.ServerBootstrap;
import org.infinispan.rest.configuration.ExtendedHeaders;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.jboss.as.controller.services.path.PathManager;
import org.jboss.as.security.plugins.SecurityDomainContext;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.inject.Injector;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.jboss.resteasy.plugins.server.servlet.HttpServletDispatcher;
import org.jboss.resteasy.plugins.server.servlet.ResteasyBootstrap;
import org.wildfly.extension.undertow.Host;
import org.wildfly.extension.undertow.security.JAASIdentityManagerImpl;

/**
 * A service which starts the REST web application
 *
 * @author Tristan Tarrant <ttarrant@redhat.com>
 * @since 6.0
 */
public class RestService implements Service<Deployment> {
   private static final String HOME_DIR = "jboss.home.dir";
   private static final String DEFAULT_CONTEXT_PATH = "";
   private final InjectedValue<PathManager> pathManagerInjector = new InjectedValue<PathManager>();
   private final InjectedValue<EmbeddedCacheManager> cacheManagerInjector = new InjectedValue<EmbeddedCacheManager>();
   private final InjectedValue<SecurityDomainContext> securityDomainContextInjector = new InjectedValue<SecurityDomainContext>();
   private final InjectedValue<Host> hostInjector = new InjectedValue<Host>();

   private final ModelNode config;
   private final String path;
   private final String securityDomain;
   private final String authMethod;
   private final SecurityMode securityMode;
   private PathManager.Callback.Handle callbackHandle;
   private final RestServerConfiguration configuration;
   private final DeploymentInfo deployment;
   private DeploymentManager deploymentManager;

   public RestService(ModelNode config) {
      deployment = new DeploymentInfo();
      this.config = config.clone();

      path = this.config.hasDefined(ModelKeys.CONTEXT_PATH) ? cleanContextPath(this.config.get(ModelKeys.CONTEXT_PATH).asString()) : DEFAULT_CONTEXT_PATH;
      securityDomain = config.hasDefined(ModelKeys.SECURITY_DOMAIN) ? config.get(ModelKeys.SECURITY_DOMAIN).asString() : null;
      authMethod = config.hasDefined(ModelKeys.AUTH_METHOD) ? config.get(ModelKeys.AUTH_METHOD).asString() : "BASIC";
      securityMode = config.hasDefined(ModelKeys.SECURITY_MODE) ? SecurityMode.valueOf(config.get(ModelKeys.SECURITY_MODE).asString()) : SecurityMode.READ_WRITE;

      RestServerConfigurationBuilder builder = new RestServerConfigurationBuilder();
      builder.extendedHeaders(config.hasDefined(ModelKeys.EXTENDED_HEADERS)
            ? ExtendedHeaders.valueOf(config.get(ModelKeys.EXTENDED_HEADERS).asString())
            : ExtendedHeaders.ON_DEMAND);
      configuration = builder.build();
   }

   private static String cleanContextPath(String s) {
      if (s.endsWith("/")) {
         return s.substring(0, s.length() - 1);
      } else {
         return s;
      }
   }

   /** {@inheritDoc} */
   @Override
   public synchronized void start(StartContext startContext) throws StartException {
      ROOT_LOGGER.endpointStarting("REST");
      try {
         deployment
            .setDeploymentName("REST")
            .setContextPath(path)
            .setClassLoader(this.getClass().getClassLoader())
            .addInitParameter("resteasy.resources", "org.infinispan.rest.Server")
            .addInitParameter("resteasy.use.builtin.providers", "true")
            .addListener(new ListenerInfo(ResteasyBootstrap.class))
            .addMimeMappings(new MimeMapping("html", "text/html"), new MimeMapping("jpg", "image/jpeg"))
            .addWelcomePage("index.html")
            .setResourceManager(new FileResourceManager(new File(pathManagerInjector.getValue().resolveRelativePathEntry("rest", HOME_DIR)), 1024 * 1024));
         callbackHandle = pathManagerInjector.getValue().registerCallback(HOME_DIR, PathManager.ReloadServerCallback.create(), PathManager.Event.UPDATED, PathManager.Event.REMOVED);

         // Add the default servlet for managing static content
         deployment.addServlet(new ServletInfo("default", DefaultServlet.class).addMapping("/"));

         // Add the Resteasy servlet dispatcher for handling REST requests
         deployment.addServlet(new ServletInfo("Resteasy", HttpServletDispatcher.class).addMapping("/rest/*"));

         if (securityDomain != null) {
            configureContextSecurity();
         }
         deploymentManager = hostInjector.getValue().getServer().getServletContainer().getServletContainer().addDeployment(deployment);
         deploymentManager.deploy();

         // Inject cache manager and configuration
         ServletContext servletContext = deploymentManager.getDeployment().getServletContext();
         ServerBootstrap.setCacheManager(servletContext, cacheManagerInjector.getValue());
         ServerBootstrap.setConfiguration(servletContext, configuration);

      } catch (Exception e) {
         throw ROOT_LOGGER.restContextCreationFailed(e);
      }
      try {
         HttpHandler httpHandler = deploymentManager.start();
         hostInjector.getValue().registerDeployment(deploymentManager.getDeployment(), httpHandler);
         ROOT_LOGGER.httpEndpointStarted("REST", path, "rest");
      } catch (Exception e) {
         throw ROOT_LOGGER.restContextStartFailed(e);
      }
   }

   private void configureContextSecurity() {
      SecurityConstraint constraint = new SecurityConstraint();
      WebResourceCollection webCollection = new WebResourceCollection();
      webCollection.addUrlPattern("/rest/*");
      switch (securityMode) {
      case WRITE:
         // protect all writes
         webCollection.addHttpMethods("PUT", "POST", "DELETE");
         break;
      case READ_WRITE:
         // protect all methods
         break;
      }
      constraint.addWebResourceCollection(webCollection);
      constraint.addRoleAllowed("REST");
      deployment.addSecurityConstraint(constraint);

      LoginConfig login = new LoginConfig("ApplicationRealm").addFirstAuthMethod(authMethod);
      deployment.setLoginConfig(login);
      deployment.addSecurityRole("REST");

      SecurityDomainContext securityDomainContext = securityDomainContextInjector.getValue();
      deployment.setIdentityManager(new JAASIdentityManagerImpl(securityDomainContext));

      // Commented waiting for WFLY-2553
      /*if ("SPNEGO".equals(authMethod)) {
         context.addValve(new NegotiationAuthenticator());
      }*/
   }

   public String getSecurityDomain() {
      return securityDomain;
   }

   /** {@inheritDoc} */
   @Override
   public synchronized void stop(StopContext stopContext) {
      if (callbackHandle != null) {
         callbackHandle.remove();
      }
      try {
         hostInjector.getValue().unregisterDeployment(deploymentManager.getDeployment());
         deploymentManager.stop();
      } catch (Exception e) {
         ROOT_LOGGER.contextStopFailed(e);
      }
      try {
         deploymentManager.undeploy();
         hostInjector.getValue().getServer().getServletContainer().getServletContainer().removeDeployment(deployment);
      } catch (Exception e) {
         ROOT_LOGGER.contextDestroyFailed(e);
      }
   }

   String getCacheContainerName() {
      if (!config.hasDefined(ModelKeys.CACHE_CONTAINER)) {
         return null;
      }
      return config.get(ModelKeys.CACHE_CONTAINER).asString();
   }

   /** {@inheritDoc} */
   @Override
   public synchronized Deployment getValue() throws IllegalStateException {
      if (deploymentManager == null) {
         throw new IllegalStateException();
      }
      return deploymentManager.getDeployment();
   }

   public InjectedValue<PathManager> getPathManagerInjector() {
      return pathManagerInjector;
   }

   public InjectedValue<EmbeddedCacheManager> getCacheManager() {
      return cacheManagerInjector;
   }

   public InjectedValue<SecurityDomainContext> getSecurityDomainContextInjector() {
      return securityDomainContextInjector;
   }

   public Injector<Host> getHostInjector() {
      return hostInjector;
   }
}