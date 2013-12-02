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

import java.lang.reflect.InvocationTargetException;

import javax.servlet.ServletContext;
import org.apache.catalina.Context;
import org.apache.catalina.Host;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.Loader;
import org.apache.catalina.Wrapper;
import org.apache.catalina.core.StandardContext;
import org.apache.catalina.deploy.ApplicationParameter;
import org.apache.catalina.deploy.LoginConfig;
import org.apache.catalina.deploy.SecurityCollection;
import org.apache.catalina.deploy.SecurityConstraint;
import org.apache.tomcat.InstanceManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.ServerBootstrap;
import org.infinispan.rest.configuration.ExtendedHeaders;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.jboss.as.controller.services.path.PathManager;
import org.jboss.as.security.plugins.SecurityDomainContext;
import org.jboss.as.web.VirtualHost;
import org.jboss.as.web.deployment.WebCtxLoader;
import org.jboss.as.web.security.JBossWebRealm;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.jboss.resteasy.plugins.server.servlet.HttpServletDispatcher;
import org.jboss.resteasy.plugins.server.servlet.ResteasyBootstrap;
import org.jboss.security.negotiation.NegotiationAuthenticator;

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;

/**
 * A service which starts the REST web application
 *
 * @author Tristan Tarrant <ttarrant@redhat.com>
 * @since 6.0
 */
public class RestService implements Service<Context> {
   private static final String DEFAULT_VIRTUAL_SERVER = "default-host";
   private static final String HOME_DIR = "jboss.home.dir";
   private static final String DEFAULT_CONTEXT_PATH = "";
   private final StandardContext context;
   private final InjectedValue<PathManager> pathManagerInjector = new InjectedValue<PathManager>();
   private final InjectedValue<VirtualHost> hostInjector = new InjectedValue<VirtualHost>();
   private final InjectedValue<EmbeddedCacheManager> cacheManagerInjector = new InjectedValue<EmbeddedCacheManager>();
   private final InjectedValue<SecurityDomainContext> securityDomainContextInjector = new InjectedValue<SecurityDomainContext>();
   private final ModelNode config;
   private final String virtualServer;
   private final String path;
   private final String securityDomain;
   private final String authMethod;
   private final SecurityMode securityMode;
   private PathManager.Callback.Handle callbackHandle;
   private final RestServerConfiguration configuration;

   public RestService(ModelNode config) {
      this.config = config.clone();

      context = new StandardContext();
      virtualServer = this.config.hasDefined(ModelKeys.VIRTUAL_SERVER) ? this.config.get(ModelKeys.VIRTUAL_SERVER)
            .asString() : DEFAULT_VIRTUAL_SERVER;
      path = this.config.hasDefined(ModelKeys.CONTEXT_PATH) ? cleanContextPath(this.config.get(ModelKeys.CONTEXT_PATH)
            .asString()) : DEFAULT_CONTEXT_PATH;
      securityDomain = config.hasDefined(ModelKeys.SECURITY_DOMAIN) ? config.get(ModelKeys.SECURITY_DOMAIN).asString()
            : null;
      authMethod = config.hasDefined(ModelKeys.AUTH_METHOD) ? config.get(ModelKeys.AUTH_METHOD).asString() : "BASIC";
      securityMode = config.hasDefined(ModelKeys.SECURITY_MODE) ? SecurityMode.valueOf(config.get(
            ModelKeys.SECURITY_MODE).asString()) : SecurityMode.READ_WRITE;

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
         context.setPath(path);
         context.addLifecycleListener(new RestContextConfig());

         context.setDocBase(pathManagerInjector.getValue().resolveRelativePathEntry("rest", HOME_DIR));
         callbackHandle = pathManagerInjector.getValue().registerCallback(HOME_DIR,
               PathManager.ReloadServerCallback.create(), PathManager.Event.UPDATED, PathManager.Event.REMOVED);

         final Loader loader = new WebCtxLoader(this.getClass().getClassLoader());
         Host host = hostInjector.getValue().getHost();
         loader.setContainer(host);
         context.setLoader(loader);
         context.setInstanceManager(new LocalInstanceManager());

         // Configuration for Resteasy bootstrap
         addContextApplicationParameter(context, "resteasy.resources", "org.infinispan.rest.Server");
         addContextApplicationParameter(context, "resteasy.use.builtin.providers", "true");

         // Setup the Resteasy bootstrap listener
         context.addApplicationListener(ResteasyBootstrap.class.getName());

         // Set the welcome file
         context.setReplaceWelcomeFiles(true);
         context.addWelcomeFile("index.html");

         // Add the default servlet for managing static content
         Wrapper wrapper = context.createWrapper();
         wrapper.setName("default");
         wrapper.setServletClass("org.apache.catalina.servlets.DefaultServlet");
         context.addChild(wrapper);
         context.addServletMapping("/", "default");
         context.addMimeMapping("html", "text/html");
         context.addMimeMapping("jpg", "image/jpeg");

         // Add the Resteasy servlet dispatcher for handling REST requests
         HttpServletDispatcher hsd = new HttpServletDispatcher();
         Wrapper hsdWrapper = context.createWrapper();
         hsdWrapper.setName("Resteasy");
         hsdWrapper.setServlet(hsd);
         hsdWrapper.setServletClass(hsd.getClass().getName());
         context.addChild(hsdWrapper);

         context.addServletMapping("/rest/*", "Resteasy");

         // Inject cache manager and configuration
         ServletContext servletContext = context.getServletContext();
         ServerBootstrap.setCacheManager(servletContext, cacheManagerInjector.getValue());
         ServerBootstrap.setConfiguration(servletContext, configuration);

         if (securityDomain != null) {
            configureContextSecurity();
         }

         host.addChild(context);
         context.create();
      } catch (Exception e) {
         throw ROOT_LOGGER.restContextCreationFailed(e);
      }
      try {
         context.start();
         ROOT_LOGGER.httpEndpointStarted("REST", path, "rest");
      } catch (LifecycleException e) {
         throw ROOT_LOGGER.restContextStartFailed(e);
      }
   }

   private void configureContextSecurity() {
      SecurityConstraint constraint = new SecurityConstraint();
      SecurityCollection webCollection = new SecurityCollection();
      webCollection.addPattern("/rest/*");
      switch (securityMode) {
      case WRITE:
         // protect all writes
         webCollection.addMethod("PUT");
         webCollection.addMethod("POST");
         webCollection.addMethod("DELETE");
         break;
      case READ_WRITE:
         // protect all methods
         break;
      }
      constraint.addCollection(webCollection);
      constraint.setAuthConstraint(true);
      constraint.addAuthRole("REST");
      context.addConstraint(constraint);
      LoginConfig login = new LoginConfig();
      login.setAuthMethod(authMethod);
      login.setRealmName("ApplicationRealm");
      context.setLoginConfig(login);
      context.addSecurityRole("REST");

      JBossWebRealm realm = new JBossWebRealm();
      SecurityDomainContext securityDomainContext = securityDomainContextInjector.getValue();
      realm.setAuthenticationManager(securityDomainContext.getAuthenticationManager());
      realm.setAuthorizationManager(securityDomainContext.getAuthorizationManager());
      realm.setMappingManager(securityDomainContext.getMappingManager());
      realm.setAuditManager(securityDomainContext.getAuditManager());
      context.setRealm(realm);
      context.addValve(new RestSecurityContext(path, securityDomain));
      if ("SPNEGO".equals(authMethod)) {
         context.addValve(new NegotiationAuthenticator());
      }
   }

   public String getVirtualServer() {
      return virtualServer;
   }

   public String getSecurityDomain() {
      return securityDomain;
   }

   private static void addContextApplicationParameter(Context context, String paramName, String paramValue) {
      ApplicationParameter parameter = new ApplicationParameter();
      parameter.setName(paramName);
      parameter.setValue(paramValue);
      context.addApplicationParameter(parameter);
   }

   /** {@inheritDoc} */
   @Override
   public synchronized void stop(StopContext stopContext) {
      if (callbackHandle != null) {
         callbackHandle.remove();
      }
      try {
         hostInjector.getValue().getHost().removeChild(context);
         context.stop();
      } catch (LifecycleException e) {
         ROOT_LOGGER.contextStopFailed(e);
      }
      try {
         context.destroy();
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
   public synchronized Context getValue() throws IllegalStateException {
      final Context context = this.context;
      if (context == null) {
         throw new IllegalStateException();
      }
      return context;
   }

   public InjectedValue<PathManager> getPathManagerInjector() {
      return pathManagerInjector;
   }

   public InjectedValue<VirtualHost> getHostInjector() {
      return hostInjector;
   }

   public InjectedValue<EmbeddedCacheManager> getCacheManager() {
      return cacheManagerInjector;
   }

   public InjectedValue<SecurityDomainContext> getSecurityDomainContextInjector() {
      return securityDomainContextInjector;
   }

   private static class LocalInstanceManager implements InstanceManager {

      @Override
      public Object newInstance(String className) throws IllegalAccessException, InvocationTargetException,
            InstantiationException, ClassNotFoundException {
         return Class.forName(className).newInstance();
      }

      @Override
      public Object newInstance(String fqcn, ClassLoader classLoader) throws IllegalAccessException,
            InvocationTargetException, InstantiationException, ClassNotFoundException {
         return Class.forName(fqcn, false, classLoader).newInstance();
      }

      @Override
      public Object newInstance(Class<?> c) throws IllegalAccessException, InvocationTargetException,
            InstantiationException {
         return c.newInstance();
      }

      @Override
      public void newInstance(Object o) throws IllegalAccessException, InvocationTargetException {
         throw new IllegalStateException();
      }

      @Override
      public void destroyInstance(Object o) throws IllegalAccessException, InvocationTargetException {
      }
   }
}
