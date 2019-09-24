package org.infinispan.cli.resources;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class CounterResource extends AbstractResource {
   public CounterResource(CountersResource parent, String name) {
      super(parent, name);
   }

   @Override
   public boolean isLeaf() {
      return true;
   }
}
