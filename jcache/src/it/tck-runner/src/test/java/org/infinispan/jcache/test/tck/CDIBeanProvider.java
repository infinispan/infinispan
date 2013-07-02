package org.infinispan.jcache.test.tck;

import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;

import javax.cache.annotation.BeanProvider;
import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.UnsatisfiedResolutionException;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import java.util.Set;

/**
 * The {@link javax.cache.annotation.BeanProvider} implementation.
 * This bean provider is used to provide the beans needed for the annotation tests.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 * @author Galder Zamarreño
 */
public class CDIBeanProvider implements BeanProvider {

   private final BeanManager beanManager;

   public CDIBeanProvider() {
      final WeldContainer weldContainer = new Weld().initialize();
      beanManager = weldContainer.getBeanManager();
   }

   @Override
   @SuppressWarnings("unchecked")
   public <T> T getBeanByType(Class<T> cls) {
      if (cls == null) {
         throw new NullPointerException("cls parameter cannot be null");
      }

      final CreationalContext<?> context = beanManager.createCreationalContext(null);
      final Set<Bean<?>> beans = beanManager.getBeans(cls);
      if (!beans.isEmpty()) {
         final Bean<?> bean = beanManager.resolve(beans);
         return (T) beanManager.getReference(bean, cls, context);
      }
      throw new UnsatisfiedResolutionException("There is no bean with type '" + cls + "'");
   }
}
