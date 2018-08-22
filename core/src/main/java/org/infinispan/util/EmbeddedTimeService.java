package org.infinispan.util;

import org.infinispan.commons.time.DefaultTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * The default implementation of {@link TimeService}. It does not perform any optimization and relies on {@link
 * System#currentTimeMillis()} and {@link System#nanoTime()}.
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
@Scope(Scopes.GLOBAL)
public class EmbeddedTimeService extends DefaultTimeService {
}
