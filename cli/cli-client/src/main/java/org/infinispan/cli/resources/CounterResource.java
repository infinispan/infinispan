package org.infinispan.cli.resources;

import java.io.IOException;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class CounterResource extends AbstractResource {
   public CounterResource(CountersResource parent, String name) {
      super(parent, name);
   }

   @Override
   public Iterable<String> getChildrenNames() throws IOException {
      return getConnection().getCounterValue(getParent().getParent().getName(), name);
   }

   @Override
   public boolean isLeaf() {
      return true;
   }

   @Override
   public String describe() throws IOException {
      return getConnection().describeCounter(getParent().getParent().getName(), name);
   }
}
