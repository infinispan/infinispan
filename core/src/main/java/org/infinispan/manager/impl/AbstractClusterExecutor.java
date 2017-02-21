package org.infinispan.manager.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.infinispan.commons.CacheException;
import org.infinispan.manager.ClusterExecutionPolicy;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.TopologyAwareAddress;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.remoting.transport.jgroups.JGroupsAddressCache;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.jgroups.util.Rsp;

/**
 * Abstract executor that contains code that should be shared by all
 * @author wburns
 * @since 9.0
 */
abstract class AbstractClusterExecutor<T extends ClusterExecutor> extends LocalClusterExecutor {
   protected final JGroupsTransport transport;
   protected final Address me;

   AbstractClusterExecutor(Predicate<? super Address> predicate, EmbeddedCacheManager manager,
         JGroupsTransport transport, long time, TimeUnit unit, Executor localExecutor,
         ScheduledExecutorService timeoutExecutor) {
      super(predicate, manager, localExecutor, time, unit, timeoutExecutor);
      this.transport = transport;
      this.me = Objects.requireNonNull(transport.getAddress(),
               "Transport was not started before retrieving a ClusterExecutor!");
   }

   protected abstract T sameClusterExecutor(Predicate<? super Address> predicate,
         long time, TimeUnit unit);

   protected abstract Log getLog();

   @Override
   Address getMyAddress() {
      return me;
   }

   static org.jgroups.Address convertToJGroupsAddress(Address address) {
      return ((JGroupsAddress) address).getJGroupsAddress();
   }

   void consumeResponse(Rsp<Response> resp, org.jgroups.Address target, Consumer<? super Throwable> throwableEater) {
      consumeResponse(resp, target, o -> {}, throwableEater, throwableEater);
   }

   void consumeResponse(Rsp<Response> resp, org.jgroups.Address target, Consumer<Object> resultsEater,
         Consumer<? super Throwable> throwableEater, Consumer<? super TimeoutException> timeoutEater) {
      if (resp.wasReceived()) {
         if (resp.hasException()) {
            throwableEater.accept(resp.getException());
         }
         Response ispnResponse = resp.getValue();
         if (ispnResponse != null) {
            if (ispnResponse instanceof ExceptionResponse) {
               // We extract exception as it is always wrapped remotely
               throwableEater.accept(((ExceptionResponse) ispnResponse).getException().getCause());
            } else if (ispnResponse instanceof SuccessfulResponse) {
               resultsEater.accept(((SuccessfulResponse) ispnResponse).getResponseValue());
            } else {
               throwableEater.accept(new IllegalStateException("Response was neither successful or an exception!"));
            }
         } else {
            resultsEater.accept(null);
         }
      } else if (resp.wasSuspected()) {
         throwableEater.accept(getLog().remoteNodeSuspected(JGroupsAddressCache.fromJGroupsAddress(target)));
      } else {
         timeoutEater.accept(getLog().remoteNodeTimedOut(JGroupsAddressCache.fromJGroupsAddress(target), time, unit));
      }
   }

   /**
    * @param includeMe whether or not the list returned should contain the address for the local node
    * @return the targets we should use for JGroups. This excludes the local node if it is a target.
    */
   List<org.jgroups.Address> getJGroupsTargets(boolean includeMe) {
      List<org.jgroups.Address> list;
      List<Address> ispnMembers = transport.getMembers();
      int size = ispnMembers.size();
      if (size == 0) {
         list = Collections.emptyList();
      } else {
         if (predicate == null) {
            if (size == 1) {
               Address member = ispnMembers.get(0);
               if (!includeMe && member.equals(me)) {
                  list = Collections.emptyList();
               } else {
                  list = Collections.singletonList(convertToJGroupsAddress(member));
               }
            } else {
               list = (includeMe ? ispnMembers.stream() : ispnMembers.stream().filter(a -> !a.equals(me)))
                     .map(AllClusterExecutor::convertToJGroupsAddress)
                     .collect(Collectors.toList());
            }
         } else {
            list = (includeMe ? ispnMembers.stream() : ispnMembers.stream().filter(a -> !a.equals(me)))
                  .filter(predicate)
                  .map(AllClusterExecutor::convertToJGroupsAddress)
                  .collect(Collectors.toList());
         }
      }
      return list;
   }

   @Override
   public T filterTargets(Predicate<? super Address> predicate) {
      return sameClusterExecutor(predicate, time, unit);
   }

   @Override
   public T filterTargets(ClusterExecutionPolicy policy) throws IllegalStateException {
      if (!manager.getCacheManagerConfiguration().transport().hasTopologyInfo()) {
         throw new IllegalStateException("Topology information is not available!");
      }
      return sameClusterExecutor(a -> policy.include((TopologyAwareAddress) me,
            (TopologyAwareAddress) a), time, unit);
   }

   @Override
   public T filterTargets(ClusterExecutionPolicy policy, Predicate<? super Address> predicate) throws IllegalStateException {
      if (!manager.getCacheManagerConfiguration().transport().hasTopologyInfo()) {
         throw new IllegalStateException();
      }
      return sameClusterExecutor(a -> policy.include((TopologyAwareAddress) me,
            (TopologyAwareAddress) a) && predicate.test(a), time, unit);
   }

   @Override
   public T filterTargets(Collection<Address> addresses) {
      return filterTargets(addresses::contains);
   }

   @Override
   public T noFilter() {
      if (predicate == null) {
         return (T) this;
      }
      return sameClusterExecutor(null, time, unit);
   }

   @Override
   public T timeout(long time, TimeUnit unit) {
      if (time <= 0) {
         throw new IllegalArgumentException("Time must be greater than 0!");
      }
      Objects.requireNonNull(unit, "TimeUnit must be non null!");
      if (this.time == time && this.unit == unit) {
         return (T) this;
      }
      return sameClusterExecutor(predicate, time, unit);
   }
}
