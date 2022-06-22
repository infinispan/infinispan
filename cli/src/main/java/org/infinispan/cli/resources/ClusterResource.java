package org.infinispan.cli.resources;

import org.infinispan.cli.logging.Messages;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ClusterResource extends AbstractResource {
   static final String NAME = "cluster";

   ClusterResource(Resource parent) {
      super(parent, NAME);
   }

   public Iterable<String> getChildrenNames() {
      return getConnection().getClusterNodes();
   }

   @Override
   public Resource getChild(String name) {
      if (Resource.PARENT.equals(name)) {
         return parent;
      } else if (getConnection().getClusterNodes().contains(name)) {
         return new NodeResource(this, name);
      } else {
         throw Messages.MSG.noSuchResource(name);
      }
   }
}
