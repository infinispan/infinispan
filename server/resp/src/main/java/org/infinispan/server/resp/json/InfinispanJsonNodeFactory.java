package org.infinispan.server.resp.json;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

public class InfinispanJsonNodeFactory extends JsonNodeFactory {
   @Override
   public ArrayNode arrayNode() {
      return new InfinispanJacksonArrayNode(this);
   }

   @Override
   public ArrayNode arrayNode(int capacity) {
      return new InfinispanJacksonArrayNode(this, capacity);
   }
}
