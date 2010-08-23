package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reads more keys at a time. Specified <a href="http://community.jboss.org/wiki/HotRodBulkGet-Design">here</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class BulkGetOperation extends RetryOnFailureOperation {

   private final int entryCount;

   public BulkGetOperation(TransportFactory transportFactory, byte[] cacheName, AtomicInteger topologyId, Flag[] flags, int entryCount) {
      super(transportFactory, cacheName, topologyId, flags);
      this.entryCount = entryCount;
   }
   
   @Override
   protected Transport getTransport(int retryCount) {
      return transportFactory.getTransport();
   }

   @Override
   protected Object executeOperation(Transport transport) {
      long messageId = writeHeader(transport, BULK_GET_REQUEST);
      transport.writeVInt(entryCount);
      transport.flush();
      readHeaderAndValidate(transport, messageId, BULK_GET_RESPONSE);
      HashMap result = new HashMap();
      while ( transport.readByte() == 1) { //there's more!
         result.put(transport.readArray(), transport.readArray());
      }
      return result;
   }
}
