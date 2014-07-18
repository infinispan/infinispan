package org.infinispan.spring.provider;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.spring.AbstractRemoteCacheManagerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

import java.util.Properties;

/**
 * <p>
 * A {@link org.springframework.beans.factory.FactoryBean <code>FactoryBean</code>} for creating an
 * {@link org.infinispan.spring.provider.SpringRemoteCacheManager
 * <code>SpringRemoteCacheManager</code>} instance.
 * </p>
 * <strong>Configuration</strong><br/>
 * <p>
 * A <code>SpringRemoteCacheManager</code> is configured through a {@link java.util.Properties
 * <code>Properties</code>} object. For an exhaustive list of valid properties to be used see
 * <code>RemoteCacheManager</code>'s {@link org.infinispan.client.hotrod.RemoteCacheManager
 * javadocs}. This <code>FactoryBean</code> provides means to either
 * {@link #setConfigurationProperties(Properties) inject} a user-defined <code>Properties</code>
 * instance or to
 * {@link #setConfigurationPropertiesFileLocation(org.springframework.core.io.Resource) set} the
 * location of a properties file to load those properties from. Note that it is <em>illegal</em> to
 * use both mechanisms simultaneously.
 * </p>
 * <p>
 * Alternatively or in combination with
 * {@link #setConfigurationPropertiesFileLocation(org.springframework.core.io.Resource) setting} the
 * location of a <code>Properties</code> file to load the configuration from, this
 * <code>FactoryBean</code> provides (typed) setters for all configuration settings. Settings thus
 * defined take precedence over those defined in the injected <code>Properties</code> instance. This
 * flexibility enables users to use e.g. a company-wide <code>Properties</code> file containing
 * default settings while simultaneously overriding select settings whenever special requirements
 * warrant this.<br/>
 * Note that it is illegal to use setters in conjunction with
 * {@link #setConfigurationProperties(Properties) injecting} a <code>Properties</code> instance.
 * </p>
 * <p>
 * In addition to creating a <code>SpringRemoteCacheManager</code> this <code>FactoryBean</code>
 * does also control that <code>SpringRemoteCacheManager</code>'s lifecycle by shutting it down when
 * the enclosing Spring application context is closed. It is therefore advisable to <em>always</em>
 * use this <code>FactoryBean</code> when creating an <code>SpringRemoteCacheManager</code>.
 * </p>
 *
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 *
 * @see org.infinispan.client.hotrod.RemoteCacheManager
 * @see #destroy()
 */
public class SpringRemoteCacheManagerFactoryBean extends AbstractRemoteCacheManagerFactory
      implements FactoryBean<SpringRemoteCacheManager>, InitializingBean, DisposableBean {

   private SpringRemoteCacheManager springRemoteCacheManager;

   // ------------------------------------------------------------------------
   // org.springframework.beans.factory.InitializingBean
   // ------------------------------------------------------------------------

   /**
    * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
    */
   @Override
   public void afterPropertiesSet() throws Exception {
      assertCorrectlyConfigured();
      this.logger.info("Creating new instance of RemoteCacheManager ...");
      final Properties configurationPropertiesToUse = configurationProperties();
      final RemoteCacheManager nativeRemoteCacheManager = new RemoteCacheManager(
            configurationPropertiesToUse, this.startAutomatically);
      this.springRemoteCacheManager = new SpringRemoteCacheManager(nativeRemoteCacheManager);
      this.logger.info("Finished creating new instance of RemoteCacheManager");
   }

   // ------------------------------------------------------------------------
   // org.springframework.beans.factory.FactoryBean
   // ------------------------------------------------------------------------

   /**
    * @see org.springframework.beans.factory.FactoryBean#getObject()
    */
   @Override
   public SpringRemoteCacheManager getObject() throws Exception {
      return this.springRemoteCacheManager;
   }

   /**
    * @see org.springframework.beans.factory.FactoryBean#getObjectType()
    */
   @Override
   public Class<? extends SpringRemoteCacheManager> getObjectType() {
      return this.springRemoteCacheManager != null ? this.springRemoteCacheManager.getClass()
            : SpringRemoteCacheManager.class;
   }

   /**
    * Always return <code>true</code>.
    *
    * @see org.springframework.beans.factory.FactoryBean#isSingleton()
    */
   @Override
   public boolean isSingleton() {
      return true;
   }

   // ------------------------------------------------------------------------
   // org.springframework.beans.factory.DisposableBean
   // ------------------------------------------------------------------------

   /**
    * {@link org.infinispan.client.hotrod.RemoteCacheManager#stop() <code>stop</code>} the
    * <code>RemoteCacheManager</code> created by this factory.
    *
    * @see org.springframework.beans.factory.DisposableBean#destroy()
    */
   @Override
   public void destroy() throws Exception {
      // Being paranoid
      if (this.springRemoteCacheManager != null) {
         this.springRemoteCacheManager.stop();
      }
   }
}
