package org.infinispan.commons.dataconversion;

/**
 * @since 9.2
 * @deprecated since 12.1, to be removed in a future version.
 */
@Deprecated(forRemoval=true, since = "12.1")
public interface EncoderIds {
   short NO_ENCODER = 0;
   short IDENTITY = 1;
   short BINARY = 2;
   short UTF8 = 3;
}
