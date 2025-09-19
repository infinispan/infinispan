package org.infinispan.spring.embedded.support;

import java.lang.invoke.MethodHandles;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.StringUtils;

/**
 * <p>
 * A {@link FactoryBean <code>FactoryBean</code>} for creating a
 * native {@link #setCacheName(String) named} Infinispan {@link Cache
 * <code>org.infinispan.Cache</code>}, delegating to a
 * {@link #setInfinispanEmbeddedCacheManager(EmbeddedCacheManager) <code>configurable</code>}
 * {@link EmbeddedCacheManager
 * <code>org.infinispan.manager.EmbeddedCacheManager</code>}. If no cache name is explicitly set,
 * this <code>FactoryBean</code>'s {@link #setBeanName(String) <code>beanName</code>} will be used
 * instead.
 * </p>
 * <p>
 * Beyond merely creating named <code>Cache</code> instances, this <code>FactoryBean</code> offers
 * great flexibility in configuring those <code>Caches</code>. It has setters for all non-global
 * configuration settings, i.e. all settings that are specific to a single <code>Cache</code>. The
 * configuration settings thus defined override those settings obtained from the
 * <code>EmbeddedCacheManager</code>.
 * </p>
 * <p>
 * There are different configuration {@link #setConfigurationTemplateMode(String)
 * <code>modes</code>} that control with what <code>Configuration</code> to start before further
 * customizing it as described above:
 * <ul>
 * <li>
 * <code>NONE</code>: Configuration starts with a new <code>Configuration</code> instance.
 * Note that this mode may only be used if no named configuration having the same name as the <code>Cache</code>
 * to be created already exists. It is therefore illegal to use this mode to create a <code>Cache</code> named, say,
 * &quot;cacheName&quot; if the configuration file used to configure the
 * <code>EmbeddedCacheManager</code> contains a configuration section named
 * &quot;cacheName&quot;.</li>
 * <li>
 * <code>DEFAULT</code>: Configuration starts with the <code>EmbeddedCacheManager</code>'s
 * <em>default</em> <code>Configuration</code> instance, i.e. the configuration settings defined
 * in its configuration file's default section. Note that this mode may only be used if no named configuration
 * having the same name as the <code>Cache</code> to be created already exists. It is therefore illegal to use
 * this mode to create a <code>Cache</code> named, say, &quot;cacheName&quot; if the configuration file used
 * to configure the <code>EmbeddedCacheManager</code> contains a configuration section named
 * &quot;cacheName&quot;.</li>
 * <li>
 * <code>CUSTOM</code>: This is where a user will provide a custom-built <code>ConfigurationBuilder</code>
 * object which will be used to configure a <code>Cache</code> instance. If a {@link #setCacheName(String)} has
 * already been called, then that name will be used.
 * </li>
 * <li>
 * <code>NAMED</code>: Configuration starts with the <code>EmbeddedCacheManager</code>'s
 * <code>Configuration</code> instance having the same name as the <code>Cache</code> to be
 * created. For a <code>Cache</code> named, say, &quot;cacheName&quot; this is the configuration
 * section named &quot;cacheName&quot; as defined in the <code>EmbeddedCacheManager</code>'s
 * configuration file. Note that this mode is only useful if such a named configuration section does indeed exist.
 * Otherwise, it is equivalent to using
 * <code>DEFAULT</code>.</li>
 * </ul>
 * <p>
 * In addition to creating a named <code>Cache</code> this <code>FactoryBean</code> does also
 * control that <code>Cache</code>' {@link org.infinispan.commons.api.Lifecycle lifecycle} by shutting
 * it down when the enclosing Spring application context is closed. It is therefore advisable to
 * <em>always</em> use this <code>FactoryBean</code> when creating a named <code>Cache</code>.
 * </p>
 *
 * @author Olaf Bergner
 * @author Marius Bogoevici
 *
 */
public class InfinispanNamedEmbeddedCacheFactoryBean<K, V> implements FactoryBean<Cache<K, V>>,
                                                                      BeanNameAware, InitializingBean, DisposableBean {

   /**
    * <p>
    * Defines how to configure a new named cache produced by this <code>FactoryBean</code>:
    * <ul>
    * <li>
    * <code>NONE</code>: Configuration starts with a new <code>Configuration</code> instance.
    * Note that this mode may only be used if no named configuration having the same name as the <code>Cache</code>
    * to be created already exists. It is therefore illegal to use this mode to create a <code>Cache</code> named, say,
    * &quot;cacheName&quot; if the configuration file used to configure the
    * <code>EmbeddedCacheManager</code> contains a configuration section named
    * &quot;cacheName&quot;.</li>
    * <li>
    * <code>DEFAULT</code>: Configuration starts with the <code>EmbeddedCacheManager</code>'s
    * <em>default</em> <code>Configuration</code> instance, i.e. the configuration settings defined
    * in its configuration file's default section. Note that this mode may only be used if no named configuration
    * having the same name as the <code>Cache</code> to be created already exists. It is therefore illegal to use
    * this mode to create a <code>Cache</code> named, say, &quot;cacheName&quot; if the configuration file used
    * to configure the <code>EmbeddedCacheManager</code> contains a configuration section named
    * &quot;cacheName&quot;.</li>
    * <li>
    * <code>CUSTOM</code>: This is where a user will provide a custom-built <code>ConfigurationBuilder</code>
    * object which will be used to configure a <code>Cache</code> instance. If a {@link #setCacheName(String)} has
    * already been called, then that name will be used.
    * </li>
    * <li>
    * <code>NAMED</code>: Configuration starts with the <code>EmbeddedCacheManager</code>'s
    * <code>Configuration</code> instance having the same name as the <code>Cache</code> to be
    * created. For a <code>Cache</code> named, say, &quot;cacheName&quot; this is the configuration
    * section named &quot;cacheName&quot; as defined in the <code>EmbeddedCacheManager</code>'s
    * configuration file. Note that this mode is only useful if such a named configuration section does indeed exist.
    * Otherwise, it is equivalent to using
    * <code>DEFAULT</code>.</li>
    * </ul>
    * </p>
    *
    * @author Olaf Bergner
    *
    */
   enum ConfigurationTemplateMode {

      NONE,

      DEFAULT,

      CUSTOM,

      NAMED
   }

   private static final Log logger = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private EmbeddedCacheManager infinispanEmbeddedCacheManager;

   private ConfigurationTemplateMode configurationTemplateMode = ConfigurationTemplateMode.NAMED;

   private String cacheName;

   private String beanName;

   private Cache<K, V> infinispanCache;

   private ConfigurationBuilder builder = null;

   // ------------------------------------------------------------------------
   // org.springframework.beans.factory.InitializingBean
   // ------------------------------------------------------------------------

   /**
    * @see InitializingBean#afterPropertiesSet()
    */
   @Override
   public void afterPropertiesSet() throws Exception {
      if (this.infinispanEmbeddedCacheManager == null) {
         throw new IllegalStateException("No Infinispan EmbeddedCacheManager has been set");
      }
      logger.info("Initializing named Infinispan embedded cache ...");
      final String effectiveCacheName = obtainEffectiveCacheName();
      this.infinispanCache = configureAndCreateNamedCache(effectiveCacheName);
      logger.info("New Infinispan embedded cache [" + this.infinispanCache + "] initialized");
   }

   private Cache<K, V> configureAndCreateNamedCache(final String cacheName) {
      switch (this.configurationTemplateMode) {
         case NONE:
            logger.debug("ConfigurationTemplateMode is NONE: using a fresh Configuration");
            if (this.infinispanEmbeddedCacheManager.getCacheNames().contains(cacheName)) {
               throw new IllegalStateException("Cannot use ConfigurationTemplateMode NONE: a cache named [" + cacheName + "] has already been defined.");
            }
            builder = new ConfigurationBuilder();
            this.infinispanEmbeddedCacheManager.defineConfiguration(cacheName, builder.build());
            break;
         case NAMED:
            logger.debug("ConfigurationTemplateMode is NAMED: using a named Configuration [" + cacheName + "]");
            break;
         case DEFAULT:
            logger.debug("ConfigurationTemplateMode is DEFAULT.");
            if (this.infinispanEmbeddedCacheManager.getCacheNames().contains(cacheName)) {
               throw new IllegalStateException("Cannot use ConfigurationTemplateMode DEFAULT: a cache named [" + cacheName + "] has already been defined.");
            }
            break;
         case CUSTOM:
            logger.debug("ConfigurationTemplateMode is CUSTOM. Using the provided ConfigurationBuilder.");
            if (this.builder == null) {
               throw new IllegalStateException("There has not been a ConfigurationBuilder provided. There has to be one " +
                                                     "provided for ConfigurationTemplateMode CUSTOM.");
            }
            this.infinispanEmbeddedCacheManager.defineConfiguration(cacheName, builder.build());
            break;
         default:
            throw new IllegalStateException("Unknown ConfigurationTemplateMode: " + this.configurationTemplateMode);
      }

      return this.infinispanEmbeddedCacheManager.getCache(cacheName);
   }

   private String obtainEffectiveCacheName() {
      if (StringUtils.hasText(this.cacheName)) {
         if (logger.isDebugEnabled()) {
            logger.debug("Using custom cache name [" + this.cacheName + "]");
         }
         return this.cacheName;
      } else {
         if (logger.isDebugEnabled()) {
            logger.debug("Using bean name [" + this.beanName + "] as cache name");
         }
         return this.beanName;
      }
   }

   // ------------------------------------------------------------------------
   // org.springframework.beans.factory.FactoryBean
   // ------------------------------------------------------------------------

   /**
    * @see FactoryBean#getObject()
    */
   @Override
   public Cache<K, V> getObject() throws Exception {
      return this.infinispanCache;
   }

   /**
    * @see FactoryBean#getObjectType()
    */
   @Override
   public Class<? extends Cache> getObjectType() {
      return this.infinispanCache != null ? this.infinispanCache.getClass() : Cache.class;
   }

   /**
    * Always returns <code>true</code>.
    *
    * @return Always <code>true</code>
    *
    * @see FactoryBean#isSingleton()
    */
   @Override
   public boolean isSingleton() {
      return true;
   }

   // ------------------------------------------------------------------------
   // org.springframework.beans.factory.BeanNameAware
   // ------------------------------------------------------------------------

   /**
    * @see BeanNameAware#setBeanName(String)
    */
   @Override
   public void setBeanName(final String name) {
      this.beanName = name;
   }

   // ------------------------------------------------------------------------
   // org.springframework.beans.factory.DisposableBean
   // ------------------------------------------------------------------------

   /**
    * Shuts down the <code>org.infinispan.Cache</code> created by this <code>FactoryBean</code>.
    *
    * @see DisposableBean#destroy()
    * @see Cache#stop()
    */
   @Override
   public void destroy() throws Exception {
      // Probably being paranoid here ...
      if (this.infinispanCache != null) {
         this.infinispanCache.stop();
      }
   }

   // ------------------------------------------------------------------------
   // Properties
   // ------------------------------------------------------------------------

   /**
    * <p>
    * Sets the {@link Cache#getName() name} of the {@link Cache
    * <code>org.infinispan.Cache</code>} to be created. If no explicit <code>cacheName</code> is
    * set, this <code>FactoryBean</code> will use its {@link #setBeanName(String)
    * <code>beanName</code>} as the <code>cacheName</code>.
    * </p>
    *
    * @param cacheName
    *           The {@link Cache#getName() name} of the {@link Cache
    *           <code>org.infinispan.Cache</code>} to be created
    */
   public void setCacheName(final String cacheName) {
      this.cacheName = cacheName;
   }

   /**
    * <p>
    * Sets the {@link EmbeddedCacheManager
    * <code>org.infinispan.manager.EmbeddedCacheManager</code>} to be used for creating our
    * {@link Cache <code>Cache</code>} instance. Note that this is a
    * <b>mandatory</b> property.
    * </p>
    *
    * @param infinispanEmbeddedCacheManager
    *           The {@link EmbeddedCacheManager
    *           <code>org.infinispan.manager.EmbeddedCacheManager</code>} to be used for creating
    *           our {@link Cache <code>Cache</code>} instance
    */
   public void setInfinispanEmbeddedCacheManager(
         final EmbeddedCacheManager infinispanEmbeddedCacheManager) {
      this.infinispanEmbeddedCacheManager = infinispanEmbeddedCacheManager;
   }

   /**
    * @param configurationTemplateMode
    * @throws IllegalArgumentException
    */
   public void setConfigurationTemplateMode(final String configurationTemplateMode)
         throws IllegalArgumentException {
      this.configurationTemplateMode = ConfigurationTemplateMode.valueOf(configurationTemplateMode);
   }

   /**
    * API to introduce a customised {@link ConfigurationBuilder} that will override the default configurations
    * which are already available on this class. This can <b>only</b> be used if {@link
    * #setConfigurationTemplateMode(String)} has been set to <code>CUSTOM</code>.
    *
    * @param builder
    */
   public void addCustomConfiguration(final ConfigurationBuilder builder) {
      this.builder = builder;
   }
}
