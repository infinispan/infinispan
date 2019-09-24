package org.infinispan.cli.resources;

import org.infinispan.cli.logging.Messages;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class CountersResource extends AbstractResource {
   public static final String NAME = "counters";

   protected CountersResource(Resource parent) {
      super(parent, NAME);
   }

   @Override
   public Iterable<String> getChildrenNames() {
      return getConnection().getAvailableCounters(getParent().getName());
   }

   @Override
   public Resource getChild(String name) {
      if (Resource.PARENT.equals(name)) {
         return parent;
      } else if (getConnection().getAvailableCounters(getParent().getName()).contains(name)) {
         return new CounterResource(this, name);
      } else {
         throw Messages.MSG.noSuchResource(name);
      }
   }
}
