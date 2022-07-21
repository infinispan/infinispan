package org.infinispan.api.async;

public enum BackpressureStrategy {
   BUFFER,
   DROP,
   ERROR,
   LATEST,
   IGNORE
}
