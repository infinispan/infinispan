package org.infinispan.metadata.impl;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.InvocationRecord;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.metadata.Metadata;

/**
 * {@link org.infinispan.metadata.Metadata} implementation that must be passed to the {@link
 * org.infinispan.container.DataContainer#put(Object, Object, org.infinispan.metadata.Metadata)} when the entry to store
 * is a L1 entry.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public class L1Metadata implements Metadata {

   private final Metadata metadata;

   public L1Metadata(Metadata metadata) {
      this.metadata = metadata;
   }

   @Override
   public long lifespan() {
      return metadata.lifespan();
   }

   @Override
   public long maxIdle() {
      return metadata.maxIdle();
   }

   @Override
   public EntryVersion version() {
      return metadata.version();
   }

   @Override
   public InvocationRecord lastInvocation() {
      return metadata.lastInvocation();
   }

   @Override
   public InvocationRecord invocation(CommandInvocationId id) {
      return metadata.invocation(id);
   }

   @Override
   public Metadata.Builder builder() {
      return metadata.builder();
   }

   public Metadata metadata() {
      return metadata;
   }
}
