package org.jboss.seam.infinispan.event;

import java.lang.annotation.Annotation;

import javax.enterprise.event.Event;
import javax.enterprise.inject.spi.BeanManager;
import javax.inject.Inject;

public class AbstractEventBridge<T> {

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