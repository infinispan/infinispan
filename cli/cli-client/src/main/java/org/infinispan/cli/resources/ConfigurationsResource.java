package org.infinispan.cli.resources;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ConfigurationsResource extends AbstractResource {
   static final String NAME = "configurations";

   protected ConfigurationsResource(Resource parent) {
      super(parent, NAME);
   }
}
