package org.infinispan.cli.resources;

import java.util.Arrays;

import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.logging.Messages;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class RootResource extends AbstractResource {
   private final Connection connection;

   RootResource(Connection connection) {
      super(null, "");
      this.connection = connection;
   }

   @Override
   public Iterable<String> getChildrenNames() {
      return Arrays.asList(ContainersResource.NAME, ClusterResource.NAME, ServerResource.NAME);
   }

   @Override
   public Resource getChild(String name) {
      switch (name) {
         case ContainersResource.NAME:
            return new ContainersResource(this);
         case ClusterResource.NAME:
            return new ClusterResource(this);
         case ServerResource.NAME:
            return new ServerResource(this);
         default:
            throw Messages.MSG.noSuchResource(name);
      }
   }

   @Override
   Connection getConnection() {
      return connection;
   }
}
