package org.infinispan.xsite.commands.remote;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.TracedCommand;
import org.infinispan.factories.GlobalComponentRegistry;

/**
 * It represents a cross-site request.
 *
 * @since 15.0
 */
public interface XSiteRequest<T> extends TracedCommand {

   /**
    * This method is invoked by the receiver node.
    * <p>
    * The {@link GlobalComponentRegistry} gives access to every component managed by this cache manager.
    *
    * @param origin   The sender site.
    * @param registry The {@link GlobalComponentRegistry}.
    */
   CompletionStage<T> invokeInLocalSite(String origin, GlobalComponentRegistry registry);
}
