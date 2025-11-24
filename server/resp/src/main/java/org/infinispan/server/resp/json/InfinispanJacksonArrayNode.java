package org.infinispan.server.resp.json;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

public class InfinispanJacksonArrayNode extends ArrayNode {
   public InfinispanJacksonArrayNode(JsonNodeFactory nf) {
      super(nf);
   }

   public InfinispanJacksonArrayNode(InfinispanJsonNodeFactory nf, int capacity) {
      super(nf, capacity);
   }
}
