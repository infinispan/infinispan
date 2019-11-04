package org.infinispan.cli.resources;

import java.io.IOException;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.1
 **/
public class TaskResource extends AbstractResource {
   public TaskResource(TasksResource parent, String name) {
      super(parent, name);
   }

   @Override
   public boolean isLeaf() {
      return true;
   }

   @Override
   public String describe() throws IOException {
      return getConnection().describeTask(getParent().getParent().getName(), name);
   }
}
