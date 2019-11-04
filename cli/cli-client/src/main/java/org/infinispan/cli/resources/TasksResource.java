package org.infinispan.cli.resources;

import java.io.IOException;

import org.infinispan.cli.logging.Messages;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.1
 **/
public class TasksResource extends AbstractResource {
   public static final String NAME = "tasks";

   protected TasksResource(Resource parent) {
      super(parent, NAME);
   }

   @Override
   public Iterable<String> getChildrenNames() throws IOException {
      return getConnection().getAvailableTasks(getParent().getName());
   }

   @Override
   public Resource getChild(String name) throws IOException {
      if (Resource.PARENT.equals(name)) {
         return parent;
      } else if (getConnection().getAvailableTasks(getParent().getName()).contains(name)) {
         return new TaskResource(this, name);
      } else {
         throw Messages.MSG.noSuchResource(name);
      }
   }

   @Override
   public String describe() {
      return NAME;
   }
}
