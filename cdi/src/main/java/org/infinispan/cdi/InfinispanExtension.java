package org.infinispan.cdi;

import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.AfterBeanDiscovery;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.Extension;
import javax.enterprise.inject.spi.ProcessBean;
import javax.enterprise.inject.spi.ProcessInjectionTarget;
import javax.enterprise.inject.spi.ProcessProducer;

import org.infinispan.cdi.util.defaultbean.DefaultBeanHolder;
import org.infinispan.cdi.util.defaultbean.Installed;
import org.infinispan.cdi.util.logging.Log;
import org.infinispan.commons.logging.LogFactory;

/**
 * The Infinispan CDI extension class.
 *
 * @author Pete Muir
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class InfinispanExtension implements Extension {
   private static final Log log = LogFactory.getLog(InfinispanExtension.class, Log.class);
   private final InfinispanExtensionEmbedded embeddedExtension;
   private final InfinispanExtensionRemote remoteExtension;

   public InfinispanExtension() {
      InfinispanExtensionEmbedded e;
      try {
         e = new InfinispanExtensionEmbedded();
         log.debug("Enabling support for embedded CDI");
      } catch (Throwable t) {
         e = null;
         log.debug("Disabling support for embedded CDI");
      }
      embeddedExtension = e;
      InfinispanExtensionRemote r;
      try {
         r = new InfinispanExtensionRemote();
         log.debug("Enabling support for remote CDI");
      } catch (Throwable t) {
         r = null;
         log.debug("Disabling support for remote CDI");
      }
      remoteExtension = r;
   }

   void processProducers(@Observes ProcessProducer<?, ?> event, BeanManager beanManager) {
      if (remoteExtension != null) {
         remoteExtension.processProducers(event);
      }
      if (embeddedExtension != null) {
         embeddedExtension.processProducers(event, beanManager);
      }
   }

   <T> void processInjectionPoints(@Observes ProcessInjectionTarget<T> event, BeanManager beanManager) {
      if (remoteExtension != null) {
         remoteExtension.saveRemoteInjectionPoints(event, beanManager);
      }
   }

   void afterBeanDiscovery(@Observes AfterBeanDiscovery event, final BeanManager beanManager) {
      if (remoteExtension != null) {
         remoteExtension.registerCacheBeans(event, beanManager);
      }
      if (embeddedExtension != null) {
         embeddedExtension.registerCacheBeans(event, beanManager);
         embeddedExtension.registerInputCacheCustomBean(event, beanManager);
      }
   }

   public void observeDefaultBean(@Observes @Installed DefaultBeanHolder bean) {
      if (embeddedExtension != null) {
         embeddedExtension.observeDefaultEmbeddedCacheManagerInstalled(bean);
      }
   }

   public void processBean(@Observes ProcessBean<?> processBean) {
      if (embeddedExtension != null) {
         embeddedExtension.observeEmbeddedCacheManagerBean(processBean);
      }
   }

   public InfinispanExtensionEmbedded getEmbeddedExtension() {
      return embeddedExtension;
   }

   public InfinispanExtensionRemote getRemoteExtension() {
      return remoteExtension;
   }
}
