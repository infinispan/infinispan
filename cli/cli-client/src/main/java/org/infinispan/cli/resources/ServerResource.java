package org.infinispan.cli.resources;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ServerResource extends AbstractResource {
   public static final String NAME = "server";

   ServerResource(Resource parent) {
      super(parent, NAME);
   }
}
