package org.infinispan.cli.resources;

import java.io.IOException;
import java.util.Arrays;

import org.infinispan.cli.logging.Messages;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ContainerResource extends AbstractResource {
   ContainerResource(ContainersResource parent, String name) {
      super(parent, name);
   }

   @Override
   public Iterable<String> getChildrenNames() {
      return Arrays.asList(CachesResource.NAME, CountersResource.NAME, ConfigurationsResource.NAME, SchemasResource.NAME, TasksResource.NAME);
   }

   @Override
   public Resource getChild(String name) {
      switch (name) {
         case Resource.PARENT:
            return parent;
         case CachesResource.NAME:
            return new CachesResource(this);
         case CountersResource.NAME:
            return new CountersResource(this);
         case ConfigurationsResource.NAME:
            return new ConfigurationsResource(this);
         case SchemasResource.NAME:
            return new SchemasResource(this);
         case TasksResource.NAME:
            return new TasksResource(this);
         default:
            throw Messages.MSG.noSuchResource(name);
      }
   }

   @Override
   public String describe() throws IOException {
      return getConnection().describeContainer(name);
   }
}
