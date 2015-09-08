package org.infinispan.lucene.impl;

import java.io.IOException;

/**
 * Common interface between Lock implementations having an obtain method,
 * as it was supported in older Lucene versions and currently still useful for our design.
 */
public interface ObtainableLock {

   boolean obtain() throws IOException;

}
