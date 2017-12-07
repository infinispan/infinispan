package org.infinispan.remoting.transport.impl;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;

/**
 * Receive responses from multiple nodes, without checking that the responses are valid.
 *
 * @author Dan Berindei
 * @since 9.1
 */
public class PassthroughMapResponseCollector implements ResponseCollector<Map<Address, Response>> {
   private Map<Address, Response> map;

   public PassthroughMapResponseCollector(int expectedSize) {
      map = new HashMap<>(CollectionFactory.computeCapacity(expectedSize));
   }

   @Override
   public Map<Address, Response> addResponse(Address sender, Response response) {
      map.put(sender, response);
      return null;
   }

   @Override
   public Map<Address, Response> finish() {
      return map;
   }
}
