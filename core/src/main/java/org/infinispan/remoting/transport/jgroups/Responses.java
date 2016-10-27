package org.infinispan.remoting.transport.jgroups;

import org.infinispan.remoting.responses.Response;
import org.jgroups.Address;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReferenceArray;

import net.jcip.annotations.ThreadSafe;

/**
 * Pre-allocated holder for multiple responses. Until the response is received,
 * or node is marked as suspected/unreachable the value is <code>null</code>.
 *
 * This class is safe against concurrent access by multiple threads.
 */
@ThreadSafe
public class Responses implements Iterable<Rsp<Response>> {
   public static final Responses EMPTY = new Responses(Collections.emptyList());
   private final Address[] addresses;
   private final AtomicReferenceArray<Rsp<Response>> responses;
   private volatile boolean timedOut = false;

   public Responses(Collection<Address> addresses) {
      int size = addresses.size();
      this.addresses = addresses.toArray(new Address[size]);
      this.responses = new AtomicReferenceArray<>(size);
   }

   /**
    * Constructs a new instance with responses from all recipients marked
    * as suspected.
    *
    * @param dests Collection of recipients.
    * @return
    */
   public static Responses suspected(Collection<Address> dests) {
      Responses responses = new Responses(dests);
      for (int i = 0; i < responses.addresses.length; ++i) {
         Rsp<Response> rsp = new Rsp<>(responses.addresses[i]);
         rsp.setSuspected();
         responses.responses.set(i, rsp);
      }
      return responses;
   }

   /**
    * Constructs new instance using existing {@link RspList}. If the response
    * was not received and the node was neither marked as suspected nor unreachable
    * the response in this instance will be <code>null</code>.
    *
    * @param results Received responses.
    */
   public Responses(RspList<Response> results) {
      int size = results.size();
      this.addresses = new Address[size];
      this.responses = new AtomicReferenceArray<>(size);
      int i = 0;
      for (Rsp<Response> rsp : results) {
         addresses[i] = rsp.getSender();
         // keep non-received response as null
         if (rsp.wasReceived() || rsp.wasSuspected() || rsp.wasUnreachable()) {
            responses.set(i, rsp);
         }
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

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("Responses{");
      for (int i = 0; i < addresses.length; ++i) {
         sb.append('\n').append(addresses[i]).append(": ").append(responses.get(i));
      }
      sb.append('}');
      return sb.toString();
   }

   public void setTimedOut() {
      this.timedOut = true;
   }

   public boolean isTimedOut() {
      return timedOut;
   }
}
