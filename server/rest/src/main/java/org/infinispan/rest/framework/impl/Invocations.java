package org.infinispan.rest.framework.impl;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.infinispan.rest.framework.Invocation;
import org.infinispan.rest.framework.ResourceDescription;

/**
 *  Aggregator for {@link Invocation}.
 *
 * @since 10.0
 */
public class Invocations implements Iterable<Invocation> {

   private final List<Invocation> invocations;

   private Invocations(List<Invocation> invocations) {
      this.invocations = invocations;
   }

   @Override
   public Iterator<Invocation> iterator() {
      return invocations.iterator();
   }

   public static class Builder {

      private final ResourceDescription description;

      private InvocationImpl.Builder currentBuilder;
      List<Invocation> invocations = new LinkedList<>();

      public Builder(String group, String description) {
         this.description = new ResourceDescription(group, description);
      }

      public InvocationImpl.Builder invocation() {
         if (currentBuilder != null) {
            invocations.add(currentBuilder.build());
         }
         currentBuilder = new InvocationImpl.Builder(this);
         return currentBuilder;
      }

      public Invocations build(InvocationImpl.Builder lastBuilder) {
         invocations.add(lastBuilder.build());
         return new Invocations(invocations);
      }

      public Invocations create() {
         if (currentBuilder != null) {
            invocations.add(currentBuilder.build());
         }
         return new Invocations(invocations);
      }

      public final ResourceDescription description() {
         return description;
      }
   }
}
