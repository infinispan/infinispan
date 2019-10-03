package org.infinispan.cli.resources;

import java.io.IOException;

import org.infinispan.cli.logging.Messages;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class SchemasResource extends AbstractResource {
   public static final String NAME = "schemas";

   protected SchemasResource(Resource parent) {
      super(parent, NAME);
   }

   @Override
   public Iterable<String> getChildrenNames() throws IOException {
      return getConnection().getAvailableSchemas(getParent().getName());
   }

   @Override
   public Resource getChild(String name) {
      if (Resource.PARENT.equals(name)) {
         return parent;
      } else {
         throw Messages.MSG.noSuchResource(name);
      }
   }

   @Override
   public boolean isLeaf() {
      return true;
   }

   @Override
   public String describe() throws IOException {
      return NAME;//TODO
   }
}
