package org.infinispan.spring.remote.support;

import java.util.Properties;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.spring.remote.AbstractRemoteCacheManagerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * <p>
 * A {@link FactoryBean <code>FactoryBean</code>} for creating an
 * {@link RemoteCacheManager
 * <code>Infinispan RemoteCacheManager</code>} instance.
 * </p>
 * <b>Configuration</b><br/>
 * <p>
 * A <code>RemoteCacheManager</code> is configured through a {@link Properties
 * <code>Properties</code>} object. For an exhaustive list of valid properties to be used see
 * <code>RemoteCacheManager</code>'s {@link RemoteCacheManager
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
 * In addition to creating a <code>RemoteCacheManager</code> this <code>FactoryBean</code> does also
 * control that <code>RemoteCacheManagers</code>'s lifecycle by shutting it down when the enclosing
 * Spring application context is closed. It is therefore advisable to <em>always</em> use this
 * <code>FactoryBean</code> when creating a <code>RemoteCacheManager</code>.
 * </p>
 *
 * @author Olaf Bergner
 *
 * @see RemoteCacheManager
 * @see #destroy()
 */
public class InfinispanRemoteCacheManagerFactoryBean extends AbstractRemoteCacheManagerFactory
      implements FactoryBean<RemoteCacheManager>, InitializingBean, DisposableBean {

   private RemoteCacheManager nativeRemoteCacheManager;

   // ------------------------------------------------------------------------
   // org.springframework.beans.factory.InitializingBean
   // ------------------------------------------------------------------------

   /**
    * @see InitializingBean#afterPropertiesSet()
    */
   @Override
   public void afterPropertiesSet() throws Exception {
      assertCorrectlyConfigured();
      logger.info("Creating new instance of RemoteCacheManager ...");
      final Properties configurationPropertiesToUse = configurationProperties();
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder.withProperties(configurationPropertiesToUse);
      this.nativeRemoteCacheManager = new RemoteCacheManager(clientBuilder.build(),
                                                             this.startAutomatically);
      logger.info("Finished creating new instance of RemoteCacheManager");
   }

   // ------------------------------------------------------------------------
   // org.springframework.beans.factory.FactoryBean
   // ------------------------------------------------------------------------

   /**
    * @see FactoryBean#getObject()
    */
   @Override
   public RemoteCacheManager getObject() throws Exception {
      return this.nativeRemoteCacheManager;
   }

   /**
    * @see FactoryBean#getObjectType()
    */
   @Override
   public Class<? extends RemoteCacheManager> getObjectType() {
      return this.nativeRemoteCacheManager != null ? this.nativeRemoteCacheManager.getClass()
            : RemoteCacheManager.class;
   }

   /**
    * Always return <code>true</code>.
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
    * {@link RemoteCacheManager#stop() <code>stop</code>} the
    * <code>RemoteCacheManager</code> created by this factory.
    *
    * @see DisposableBean#destroy()
    */
   @Override
   public void destroy() throws Exception {
      // Being paranoid
      if (this.nativeRemoteCacheManager != null) {
         this.nativeRemoteCacheManager.stop();
      }
   }
}
