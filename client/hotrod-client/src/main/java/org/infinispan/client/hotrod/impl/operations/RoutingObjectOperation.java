package org.infinispan.client.hotrod.impl.operations;

public class RoutingObjectOperation<E> extends DelegatingHotRodOperation<E> {
   private final Object routingObject;
   public RoutingObjectOperation(HotRodOperation<E> delegate, Object routingObject) {
      super(delegate);
      this.routingObject = routingObject;
   }

   @Override
   public Object getRoutingObject() {
      return routingObject;
   }
}
