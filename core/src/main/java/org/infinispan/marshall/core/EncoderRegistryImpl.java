package org.infinispan.marshall.core;

/**
 * An extension of {@link org.infinispan.marshall.core.impl.EncoderRegistryImpl} provided for backwards compatibility only.
 * Users should perform all operations via the {@link EncoderRegistry} interface.
 *
 * @author Ryan Emerson
 * @deprecated since 10.0 for internal use only, will be removed in a future release.
 */
@Deprecated
public class EncoderRegistryImpl extends org.infinispan.marshall.core.impl.EncoderRegistryImpl {
}
