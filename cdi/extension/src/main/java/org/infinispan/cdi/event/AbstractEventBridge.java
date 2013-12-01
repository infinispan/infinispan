package org.infinispan.cdi.event;

import javax.enterprise.event.Event;
import javax.enterprise.inject.spi.BeanManager;
import javax.inject.Inject;
import java.lang.annotation.Annotation;

/**
 * @author Pete Muir
 */
public abstract class AbstractEventBridge<T> {

   @Inject
   private Event<T> baseEvent;

   @Inject
   private BeanManager beanManager;

   protected Event<T> getBaseEvent() {
      return baseEvent;
   }

   protected boolean hasObservers(T event, Annotation[] qualifiers) {
      return !beanManager.resolveObserverMethods(event, qualifiers).isEmpty();
   }
}
