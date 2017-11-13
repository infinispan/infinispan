package org.infinispan.persistence.remote.upgrade;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.client.hotrod.impl.iteration.RemoteCloseableIterator;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;

/**
 * A remote iterator for testing that does a callback when certain keys are reached.
 */
class CallbackRemoteIterator<E> extends RemoteCloseableIterator<E> {

   private Set<Object> callBackKeys = new HashSet<>();
   private IterationCallBack callback;

   CallbackRemoteIterator(OperationsFactory operationsFactory, int batchSize, Set<Integer> segments, boolean metadata) {
      super(operationsFactory, new GenericJBossMarshaller(), batchSize, segments, metadata);
   }

   @Override
   public void close() {
      super.close();
   }

   @Override
   public boolean hasNext() {
      return super.hasNext();
   }

   @Override
   public Map.Entry<Object, E> next() {
      Map.Entry<Object, E> next = super.next();
      Object key = unmarshall(next.getKey());
      if (callBackKeys.contains(key)) {
         callback.iterationReached(key);
      }
      return next;
   }

   private Object unmarshall(Object key) {
      try {
         return marshaller.objectFromByteBuffer((byte[]) key);
      } catch (IOException | ClassNotFoundException e) {
         e.printStackTrace();
      }
      return key;
   }

   void addCallback(IterationCallBack callback, Object... keys) {
      Arrays.stream(keys).forEach(callBackKeys::add);
      this.callback = callback;
   }
}
