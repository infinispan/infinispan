package org.infinispan.cli.completers;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
public class ExposeCompleter extends EnumCompleter<ExposeCompleter.Expose> {

   public enum Expose {
      LoadBalancer,
      NodePort,
      Route;
   }

   public ExposeCompleter() {
      super(Expose.class);
   }
}
