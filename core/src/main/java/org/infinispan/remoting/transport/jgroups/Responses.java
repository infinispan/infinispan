package org.infinispan.remoting.transport.jgroups;

import org.infinispan.remoting.responses.Response;
import org.jgroups.Address;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class Responses implements Iterable<Rsp<Response>> {
   public static final Responses EMPTY = new Responses(Collections.emptyList());
   private final Address[] addresses;
   private final AtomicReferenceArray<Rsp<Response>> responses;

   public Responses(Collection<Address> addresses) {
      int size = addresses.size();
      this.addresses = addresses.toArray(new Address[size]);
      this.responses = new AtomicReferenceArray<>(size);
   }

   public Responses(List<Address> dests, boolean suspected) {
      this(dests);
      if (suspected) {
         for (int i = 0; i < addresses.length; ++i) {
            Rsp<Response> rsp = new Rsp<>(addresses[i]);
            rsp.setSuspected();
            responses.set(i, rsp);
         }
      }
   }

   public Responses(RspList<Response> results) {
      int size = results.size();
      this.addresses = new Address[size];
      this.responses = new AtomicReferenceArray<>(size);
      int i = 0;
      for (Rsp<Response> rsp : results) {
         addresses[i] = rsp.getSender();
         responses.set(i, rsp);
         ++i;
      }
   }

   public void addResponse(Rsp rsp) {
      Address sender = rsp.getSender();
      for (int i = 0; i < addresses.length; ++i) {
         if (addresses[i].equals(sender)) {
            if (!responses.compareAndSet(i, null, rsp)) {
               throw new IllegalArgumentException("Response from " + sender + " already received: " + responses.get(i));
            }
         }
      }
   }

   public boolean isMissingResponses() {
      for (int i = 0; i < responses.length(); ++i) {
         if (responses.get(i) == null) return true;
      }
      return false;
   }

   public int size() {
      return addresses.length;
   }

   @Override
   public Iterator<Rsp<Response>> iterator() {
      return new Iterator<Rsp<Response>>() {
         int i = 0;

         @Override
         public boolean hasNext() {
            return i < responses.length();
         }

         @Override
         public Rsp<Response> next() {
            if (hasNext()) {
               return responses.get(i++);
            } else {
               throw new NoSuchElementException();
            }
         }
      };
   }
}
