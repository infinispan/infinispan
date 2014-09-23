package org.infinispan.distexec;

import java.util.List;

import org.infinispan.remoting.transport.Address;

/**
 * As {@link DistributedTask} might potentially fail on subset of executing nodes FailureContext
 * provides details of such task failure. FailureContext has a scope of a node where the task
 * failed.
 *
 * @author Vladimir Blagojevic
 * @since 5.2
 */
public interface FailoverContext {

   /**
    * Returns an Address of the node where the task failed
    *
    * @return the Address of the failed execution location
    */
   Address executionFailureLocation();

   /**
    * Returns a list of candidates for possible repeated execution governed by installed
    * {@link DistributedTaskFailoverPolicy}
    *
    * @return an Address list of possible execution candidates
    */
   List<Address> executionCandidates();

   /**
    * Returns the Throwable which was the cause of the task failure. This includes both system
    * exception related to Infinispan transient failures (node crash, transient errors etc) as well
    * as application level exceptions. Returned Throwable will most likely contain the chain of
    * Exceptions that interested clients can inspect and, if desired, find the root cause of the
    * returned Throwable
    *
    * @see Throwable#getCause() API to recursively traverse the Exception chain
    *
    * @return the Throwable that caused task failure on the particular Infinispan node
    */
   Throwable cause();

   /**
    * Returns a list of input keys for this task. Note that this method does not return all of the
    * keys used as input for {@link DistributedTask} but rather only the input keys used as input
    * for a part of that task where the execution failed
    *
    * @param <K>
    * @return the list of input keys if any
    */
   <K> List<K> inputKeys();
}
