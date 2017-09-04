package org.infinispan.configuration.cache;

/**
 * Specifies when is a node allowed to acquire a bias on an entry, serving further reads
 * to the same key locally (despite not being an owner).
 */
public enum BiasAcquisition {
   /**
    * The bias is never acquired.
    */
   NEVER,
   /**
    * Bias is acquired by the writing entry.
    */
   ON_WRITE,
   /**
    * Bias is acquired when the entry is read
    * TODO: Not implemented yet
    */
   ON_READ,
}
