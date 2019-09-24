package org.infinispan.cli.resources;

import org.infinispan.cli.logging.Messages;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class CachesResource extends AbstractResource {
   public static final String NAME = "caches";

   protected CachesResource(Resource parent) {
      super(parent, NAME);
   }

   @Override
   public Iterable<String> getChildrenNames() {
      return getConnection().getAvailableCaches(getParent().getName());
   }

   @Override
   public Resource getChild(String name) {
      if (Resource.PARENT.equals(name)) {
         return parent;
      } else if (getConnection().getAvailableCaches(getParent().getName()).contains(name)) {
         return new CacheResource(this, name);
      } else {
         throw Messages.MSG.noSuchResource(name);
      }
   }
}
