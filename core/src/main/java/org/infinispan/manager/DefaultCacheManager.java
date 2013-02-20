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
package org.infinispan.manager;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.LegacyConfigurationAdaptor;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.LegacyGlobalConfigurationAdaptor;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.lifecycle.Lifecycle;
import org.infinispan.util.FileLookupFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * A <tt>CacheManager</tt> is the primary mechanism for retrieving a {@link Cache} instance, and is often used as a
 * starting point to using the {@link Cache}.
 * <p/>
 * <tt>CacheManager</tt>s are heavyweight objects, and we foresee no more than one <tt>CacheManager</tt> being used per
 * JVM (unless specific configuration requirements require more than one; but either way, this would be a minimal and
 * finite number of instances).
 * <p/>
 * Constructing a <tt>CacheManager</tt> is done via one of its constructors, which optionally take in a {@link
 * org.infinispan.configuration.cache.Configuration} or a path or URL to a configuration XML file.
 * <p/>
 * Lifecycle - <tt>CacheManager</tt>s have a lifecycle (it implements {@link Lifecycle}) and the default constructors
 * also call {@link #start()}. Overloaded versions of the constructors are available, that do not start the
 * <tt>CacheManager</tt>, although it must be kept in mind that <tt>CacheManager</tt>s need to be started before they
 * can be used to create <tt>Cache</tt> instances.
 * <p/>
 * Once constructed, <tt>CacheManager</tt>s should be made available to any component that requires a <tt>Cache</tt>,
 * via JNDI or via some other mechanism such as an IoC container.
 * <p/>
 * You obtain <tt>Cache</tt> instances from the <tt>CacheManager</tt> by using one of the overloaded
 * <tt>getCache()</tt>, methods. Note that with <tt>getCache()</tt>, there is no guarantee that the instance you get is
 * brand-new and empty, since caches are named and shared. Because of this, the <tt>CacheManager</tt> also acts as a
 * repository of <tt>Cache</tt>s, and is an effective mechanism of looking up or creating <tt>Cache</tt>s on demand.
 * <p/>
 * When the system shuts down, it should call {@link #stop()} on the <tt>CacheManager</tt>. This will ensure all caches
 * within its scope are properly stopped as well.
 * <p/>
 * Sample usage:
 * <code>
 *    CacheManager manager = CacheManager.getInstance("my-config-file.xml");
 *    Cache&lt;String, Person&gt; entityCache = manager.getCache("myEntityCache");
 *    entityCache.put("aPerson", new Person());
 *
 *    ConfigurationBuilder confBuilder = new ConfigurationBuilder();
 *    confBuilder.clustering().cacheMode(CacheMode.REPL_SYNC);
 *    manager.defineConfiguration("myReplicatedCache", confBuilder.build());
 *    Cache&lt;String, String&gt; replicatedCache = manager.getCache("myReplicatedCache");
 * </code>
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
@SurvivesRestarts
@MBean(objectName = DefaultCacheManager.OBJECT_NAME, description = "Component that acts as a manager, factory and container for caches in the system.")
public class DefaultCacheManager extends AbstractCacheManager {
   public static final String OBJECT_NAME = "CacheManager";
   private final GlobalComponentRegistry globalComponentRegistry;

   /**
    * Constructs and starts a default instance of the CacheManager, using configuration defaults.  See {@link org.infinispan.configuration.cache.Configuration Configuration}
    * and {@link org.infinispan.configuration.global.GlobalConfiguration GlobalConfiguration} for details of these defaults.
    */
   public DefaultCacheManager() {
      this((GlobalConfiguration) null, null, true);
   }

   /**
    * Constructs a default instance of the CacheManager, using configuration defaults.  See {@link org.infinispan.configuration.cache.Configuration Configuration}
    * and {@link org.infinispan.configuration.global.GlobalConfiguration GlobalConfiguration} for details of these defaults.
    *
    * @param start if true, the cache manager is started
    */
   public DefaultCacheManager(boolean start) {
      this((GlobalConfiguration) null, null, start);
   }

   /**
    * Constructs and starts a new instance of the CacheManager, using the default configuration passed in. Uses defaults
    * for a {@link GlobalConfiguration}.  See {@link GlobalConfiguration} for details of these defaults.
    *
    * @param defaultConfiguration configuration to use as a template for all caches created
    * @deprecated Use {@link #DefaultCacheManager(org.infinispan.configuration.cache.Configuration)} instead
    */
   @Deprecated
   public DefaultCacheManager(org.infinispan.config.Configuration defaultConfiguration) {
      this(null, defaultConfiguration, true);
   }

   /**
    * Constructs and starts a new instance of the CacheManager, using the default configuration passed in.  See {@link org.infinispan.configuration.cache.Configuration Configuration}
    * and {@link org.infinispan.configuration.global.GlobalConfiguration GlobalConfiguration} for details of these defaults.
    *
    * @param defaultConfiguration configuration to use as a template for all caches created
    */
   public DefaultCacheManager(org.infinispan.configuration.cache.Configuration defaultConfiguration) {
      this(null, defaultConfiguration, true);
   }

   /**
    * Constructs a new instance of the CacheManager, using the default configuration passed in. Uses defaults for a
    * {@link org.infinispan.config.GlobalConfiguration}.  See {@link GlobalConfiguration} for details of these
    * defaults.
    *
    * @param defaultConfiguration configuration file to use as a template for all caches created
    * @param start                if true, the cache manager is started
    * @deprecated Use {@link #DefaultCacheManager(org.infinispan.configuration.cache.Configuration, boolean)} instead
    */
   @Deprecated
   public DefaultCacheManager(org.infinispan.config.Configuration defaultConfiguration, boolean start) {
      this(null, defaultConfiguration, start);
   }

   /**
    * Constructs a new instance of the CacheManager, using the default configuration passed in.  See
    * {@link org.infinispan.configuration.global.GlobalConfiguration GlobalConfiguration} for details of these defaults.
    *
    * @param defaultConfiguration configuration file to use as a template for all caches created
    * @param start                if true, the cache manager is started
    */
   public DefaultCacheManager(Configuration defaultConfiguration, boolean start) {
      this(null, defaultConfiguration, start);
   }

   /**
    * Constructs and starts a new instance of the CacheManager, using the global configuration passed in, and system
    * defaults for the default named cache configuration.  See {@link Configuration} for details of these defaults.
    *
    * @param globalConfiguration GlobalConfiguration to use for all caches created
    * @deprecated Use {@link #DefaultCacheManager(org.infinispan.configuration.global.GlobalConfiguration)} instead
    */
   @Deprecated
   public DefaultCacheManager(org.infinispan.config.GlobalConfiguration globalConfiguration) {
      this(globalConfiguration, null, true);
   }

   /**
    * Constructs and starts a new instance of the CacheManager, using the global configuration passed in, and system
    * defaults for the default named cache configuration.  See {@link org.infinispan.configuration.cache.Configuration Configuration}
    * for details of these defaults.
    *
    * @param globalConfiguration GlobalConfiguration to use for all caches created
    */
   public DefaultCacheManager(GlobalConfiguration globalConfiguration) {
      this(globalConfiguration, null, true);
   }

   /**
    * Constructs a new instance of the CacheManager, using the global configuration passed in, and system defaults for
    * the default named cache configuration.  See {@link Configuration} for details of these defaults.
    *
    * @param globalConfiguration GlobalConfiguration to use for all caches created
    * @param start               if true, the cache manager is started.
    * @deprecated Use {@link #DefaultCacheManager(org.infinispan.configuration.global.GlobalConfiguration, boolean)} instead
    */
   @Deprecated
   public DefaultCacheManager(org.infinispan.config.GlobalConfiguration globalConfiguration, boolean start) {
      this(globalConfiguration, null, start);
   }

   /**
    * Constructs a new instance of the CacheManager, using the global configuration passed in, and system defaults for
    * the default named cache configuration.  See {@link org.infinispan.configuration.cache.Configuration Configuration}
    * for details of these defaults.
    *
    * @param globalConfiguration GlobalConfiguration to use for all caches created
    * @param start               if true, the cache manager is started.
    */
   public DefaultCacheManager(GlobalConfiguration globalConfiguration, boolean start) {
      this(globalConfiguration, null, start);
   }

   /**
    * Constructs and starts a new instance of the CacheManager, using the global and default configurations passed in.
    * If either of these are null, system defaults are used.
    *
    * @param globalConfiguration  global configuration to use. If null, a default instance is created.
    * @param defaultConfiguration default configuration to use. If null, a default instance is created.
    * Use {@link #DefaultCacheManager(org.infinispan.configuration.global.GlobalConfiguration, org.infinispan.configuration.cache.Configuration)} instead
    */
   @Deprecated
   public DefaultCacheManager(org.infinispan.config.GlobalConfiguration globalConfiguration, org.infinispan.config.Configuration defaultConfiguration) {
      this(globalConfiguration, defaultConfiguration, true);
   }

   /**
    * Constructs and starts a new instance of the CacheManager, using the global and default configurations passed in.
    * If either of these are null, system defaults are used.
    *
    * @param globalConfiguration  global configuration to use. If null, a default instance is created.
    * @param defaultConfiguration default configuration to use. If null, a default instance is created.
    */
   public DefaultCacheManager(GlobalConfiguration globalConfiguration, Configuration defaultConfiguration) {
      this(globalConfiguration, defaultConfiguration, true);
   }

   /**
    * Constructs a new instance of the CacheManager, using the global and default configurations passed in. If either of
    * these are null, system defaults are used.
    *
    * @param globalConfiguration  global configuration to use. If null, a default instance is created.
    * @param defaultConfiguration default configuration to use. If null, a default instance is created.
    * @param start                if true, the cache manager is started
    * @deprecated Use {@link #DefaultCacheManager(org.infinispan.configuration.global.GlobalConfiguration, org.infinispan.configuration.cache.Configuration, boolean)} instead
    */
   @Deprecated
   public DefaultCacheManager(org.infinispan.config.GlobalConfiguration globalConfiguration, org.infinispan.config.Configuration defaultConfiguration,
                              boolean start) {
      this(LegacyGlobalConfigurationAdaptor.adapt(globalConfiguration), LegacyConfigurationAdaptor.adapt(defaultConfiguration), start);
   }

   /**
    * Constructs a new instance of the CacheManager, using the global and default configurations passed in. If either of
    * these are null, system defaults are used.
    *
    * @param globalConfiguration  global configuration to use. If null, a default instance is created.
    * @param defaultConfiguration default configuration to use. If null, a default instance is created.
    * @param start                if true, the cache manager is started
    */
   public DefaultCacheManager(GlobalConfiguration globalConfiguration, Configuration defaultConfiguration,
                              boolean start) {
      super(globalConfiguration, defaultConfiguration);
      this.globalComponentRegistry = new GlobalComponentRegistry(this.globalConfiguration, this, getCacheKeys());
      if (start)
         start();
   }

   /**
    * Constructs and starts a new instance of the CacheManager, using the configuration file name passed in. This
    * constructor first searches for the named file on the classpath, and failing that, treats the file name as an
    * absolute path.
    *
    * @param configurationFile name of configuration file to use as a template for all caches created
    *
    * @throws java.io.IOException if there is a problem with the configuration file.
    */
   public DefaultCacheManager(String configurationFile) throws IOException {
      this(configurationFile, true);
   }

   /**
    * Constructs a new instance of the CacheManager, using the configuration file name passed in. This constructor first
    * searches for the named file on the classpath, and failing that, treats the file name as an absolute path.
    *
    * @param configurationFile name of configuration file to use as a template for all caches created
    * @param start             if true, the cache manager is started
    *
    * @throws java.io.IOException if there is a problem with the configuration file.
    */
   public DefaultCacheManager(String configurationFile, boolean start) throws IOException {
      this(FileLookupFactory.newInstance().lookupFileStrict(configurationFile, Thread.currentThread().getContextClassLoader()), start);
   }

   /**
    * Constructs and starts a new instance of the CacheManager, using the input stream passed in to read configuration
    * file contents.
    *
    * @param configurationStream stream containing configuration file contents, to use as a template for all caches
    *                            created
    *
    * @throws java.io.IOException if there is a problem with the configuration stream.
    */
   public DefaultCacheManager(InputStream configurationStream) throws IOException {
      this(configurationStream, true);
   }

   /**
    * Constructs a new instance of the CacheManager, using the input stream passed in to read configuration file
    * contents.
    *
    * @param configurationStream stream containing configuration file contents, to use as a template for all caches
    *                            created
    * @param start               if true, the cache manager is started
    *
    * @throws java.io.IOException if there is a problem reading the configuration stream
    */
   public DefaultCacheManager(InputStream configurationStream, boolean start) throws IOException {
      this(new ParserRegistry(Thread.currentThread().getContextClassLoader()).parse(configurationStream), start);
   }

   /**
    * Constructs a new instance of the CacheManager, using the holder passed in to read configuration settings.
    *
    * @param holder holder containing configuration settings, to use as a template for all caches
    *                            created
    * @param start               if true, the cache manager is started
    */
   public DefaultCacheManager(ConfigurationBuilderHolder holder, boolean start) {
      super(holder);

      globalComponentRegistry = new GlobalComponentRegistry(this.globalConfiguration, this, getCacheKeys());
      if (start)
         start();
   }

   /**
    * Constructs a new instance of the CacheManager, using the two configuration file names passed in. The first file
    * contains the GlobalConfiguration configuration The second file contain the Default configuration. The third
    * filename contains the named cache configuration This constructor first searches for the named file on the
    * classpath, and failing that, treats the file name as an absolute path.
    *
    * @param start                    if true, the cache manager is started
    * @param globalConfigurationFile  name of file that contains the global configuration
    * @param defaultConfigurationFile name of file that contains the default configuration
    * @param namedCacheFile           name of file that contains the named cache configuration
    *
    * @throws java.io.IOException if there is a problem with the configuration file.
    */
   @Deprecated
   public DefaultCacheManager(String globalConfigurationFile, String defaultConfigurationFile, String namedCacheFile,
                              boolean start) throws IOException {
      super(globalConfigurationFile, defaultConfigurationFile, namedCacheFile);

      globalComponentRegistry = new GlobalComponentRegistry(this.globalConfiguration, this, getCacheKeys());
      if (start)
         start();
   }


   /**
    * Allows subclasses to create a {@link GlobalComponentRegistry}
    *
    * @return The {@link GlobalComponentRegistry}
    */
   protected GlobalComponentRegistry getGlobalComponentRegistry() {
      return this.globalComponentRegistry;
   }
}
