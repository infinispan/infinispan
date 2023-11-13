package org.infinispan.cli.resources;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

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
      return switch (name) {
         case Resource.PARENT -> parent;
         case CachesResource.NAME -> new CachesResource(this);
         case CountersResource.NAME -> new CountersResource(this);
         case ConfigurationsResource.NAME -> new ConfigurationsResource(this);
         case SchemasResource.NAME -> new SchemasResource(this);
         case TasksResource.NAME -> new TasksResource(this);
         default -> throw Messages.MSG.noSuchResource(name);
      };
   }

   @Override
   public String describe() throws IOException {
      return getConnection().describeContainer();
   }

   public static Optional<String> findContainerName(Resource resource) {
      return resource.optionalFindAncestor(ContainerResource.class).map(AbstractResource::getName);
   }
}
