package org.infinispan.marshall.core;

/**
 * An extension of {@link org.infinispan.marshall.core.impl.GlobalMarshaller} provided for backwards compatibility only.
 * Users should not reference either this class or {@link org.infinispan.marshall.core.impl.GlobalMarshaller} directly,
 * as it's for internal usage.
 *
 * @author Ryan Emerson
 * @deprecated since 10.0 for internal use only, will be removed in a future release.
 */
@Deprecated
public class GlobalMarshaller extends org.infinispan.marshall.core.impl.GlobalMarshaller {
}
