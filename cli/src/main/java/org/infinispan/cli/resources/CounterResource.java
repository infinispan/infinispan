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
      return getConnection().getCounterValue(name);
   }

   @Override
   public boolean isLeaf() {
      return true;
   }

   @Override
   public String describe() throws IOException {
      return getConnection().describeCounter(name);
   }

   public static String counterName(Resource resource) {
      return resource.findAncestor(CounterResource.class).getName();
   }
}
