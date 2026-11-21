package org.infinispan.spring.embedded.provider;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.spring.embedded.AbstractEmbeddedCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * <p>
 * A {@link FactoryBean} for creating an
 * {@link SpringEmbeddedCacheManager} instance. The location of the Infinispan configuration
 * file used to provide the default {@link org.infinispan.configuration.cache.Configuration configuration} for
 * the <code>EmbeddedCacheManager</code> instance created by this <code>FactoryBean</code> is
 * {@link #setConfigurationFileLocation(org.springframework.core.io.Resource) configurable}.
 * </p>
 * <p>
 * If no configuration file location is set the <code>SpringEmbeddedCacheManager</code> instance
 * created by this <code>FactoryBean</code> will use Infinispan's default settings. See Infinispan's
 * <a href="http://www.jboss.org/infinispan/docs">documentation</a> for what those default settings
 * are.
 * </p>
 * <p>
 * A user may further customize the <code>SpringEmbeddedCacheManager</code>'s configuration using
 * explicit setters on this <code>FactoryBean</code>. The properties thus defined will be applied
 * either to the configuration loaded from Infinispan's configuration file in case one has been
 * specified, or to a configuration initialized with Infinispan's default settings. Either way, the
 * net effect is that explicitly set configuration properties take precedence over both those loaded
 * from a configuration file as well as INFINISPAN's default settings.
 * </p>
 * <p>
 * In addition to creating an <code>SpringEmbeddedCacheManager</code> this <code>FactoryBean</code>
 * does also control that <code>SpringEmbeddedCacheManager</code>'s
 * {@link org.infinispan.commons.api.Lifecycle lifecycle} by shutting it down when the enclosing
 * Spring application context is closed. It is therefore advisable to <em>always</em> use this
 * <code>FactoryBean</code> when creating an <code>SpringEmbeddedCacheManager</code>.
 * </p>
 *
 * @author Olaf Bergner
 *
 * @see #setConfigurationFileLocation(org.springframework.core.io.Resource)
 * @see #destroy()
 * @see SpringEmbeddedCacheManager
 * @see EmbeddedCacheManager
 * @see org.infinispan.configuration.cache.Configuration
 *
 */
public class SpringEmbeddedCacheManagerFactoryBean extends AbstractEmbeddedCacheManagerFactory
      implements FactoryBean<SpringEmbeddedCacheManager>, InitializingBean, DisposableBean {
   private static final Log logger = LogFactory.getLog(SpringEmbeddedCacheManagerFactoryBean.class);

   private SpringEmbeddedCacheManager cacheManager;

   private boolean putForExternalReadOnLoad;

   private Set<String> putForExternalReadOnLoadCaches = Collections.emptySet();

   /**
    * Whether values computed by a value loader on a cache miss (for example {@code @Cacheable(sync = true)}) are
    * stored with {@link org.infinispan.Cache#putForExternalRead(Object, Object)} in all caches.
    *
    * @param putForExternalReadOnLoad {@code true} to enable it for all caches
    * @see SpringEmbeddedCache
    */
   public void setPutForExternalReadOnLoad(boolean putForExternalReadOnLoad) {
      this.putForExternalReadOnLoad = putForExternalReadOnLoad;
   }

   /**
    * Names of the caches in which values computed by a value loader on a cache miss are stored with
    * {@link org.infinispan.Cache#putForExternalRead(Object, Object)}, when it is not enabled for all caches.
    *
    * @param putForExternalReadOnLoadCaches the cache names
    * @see SpringEmbeddedCache
    */
   public void setPutForExternalReadOnLoadCaches(Set<String> putForExternalReadOnLoadCaches) {
      this.putForExternalReadOnLoadCaches = new LinkedHashSet<>(putForExternalReadOnLoadCaches);
   }

   // ------------------------------------------------------------------------
   // org.springframework.beans.factory.InitializingBean
   // ------------------------------------------------------------------------

   /**
    * @see InitializingBean#afterPropertiesSet()
    */
   @Override
   public void afterPropertiesSet() throws Exception {
      logger.info("Initializing SpringEmbeddedCacheManager instance ...");

      final EmbeddedCacheManager nativeEmbeddedCacheManager = createBackingEmbeddedCacheManager();
      final boolean allCaches = this.putForExternalReadOnLoad;
      final Set<String> selectedCaches = this.putForExternalReadOnLoadCaches;
      this.cacheManager = new SpringEmbeddedCacheManager(nativeEmbeddedCacheManager, false,
            cacheName -> allCaches || selectedCaches.contains(cacheName));

      logger.info("Successfully initialized SpringEmbeddedCacheManager instance ["
                        + this.cacheManager + "]");
   }

   // ------------------------------------------------------------------------
   // org.springframework.beans.factory.FactoryBean
   // ------------------------------------------------------------------------

   /**
    * @see FactoryBean#getObject()
    */
   @Override
   public SpringEmbeddedCacheManager getObject() throws Exception {
      return this.cacheManager;
   }

   /**
    * @see FactoryBean#getObjectType()
    */
   @Override
   public Class<? extends SpringEmbeddedCacheManager> getObjectType() {
      return this.cacheManager != null ? this.cacheManager.getClass()
            : SpringEmbeddedCacheManager.class;
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
   // org.springframework.beans.factory.DisposableBean
   // ------------------------------------------------------------------------

   /**
    * Shuts down the <code>SpringEmbeddedCacheManager</code> instance created by this
    * <code>FactoryBean</code>.
    *
    * @see DisposableBean#destroy()
    * @see SpringEmbeddedCacheManager#stop()
    */
   @Override
   public void destroy() throws Exception {
      // Probably being paranoid here ...
      if (this.cacheManager != null) {
         this.cacheManager.stop();
      }
   }
}
