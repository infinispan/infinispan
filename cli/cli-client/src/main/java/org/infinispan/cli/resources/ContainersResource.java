package org.infinispan.cli.resources;

import org.infinispan.cli.logging.Messages;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ContainersResource extends AbstractResource {
   public static final String NAME = "containers";

   ContainersResource(Resource parent) {
      super(parent, NAME);
   }

   @Override
   public Iterable<String> getChildrenNames() {
      return getConnection().getAvailableContainers();
   }

   @Override
   public Resource getChild(String name) {
      if (Resource.PARENT.equals(name)) {
         return parent;
      } else if (getConnection().getAvailableContainers().contains(name)) {
         return new ContainerResource(this, name);
      } else {
         throw Messages.MSG.noSuchResource(name);
      }
   }
}
