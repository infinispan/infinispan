package org.infinispan.cdi.embedded.event;

import java.lang.annotation.Annotation;

import jakarta.enterprise.event.Event;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.inject.Inject;

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
