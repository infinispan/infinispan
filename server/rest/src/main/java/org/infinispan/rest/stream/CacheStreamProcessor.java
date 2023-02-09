package org.infinispan.rest.stream;

/**
 * Process the elements in a {@link CacheChunkedStream}.
 * <p>
 * This processor is stateful, keeping track of the current element to process. Multiple calls to
 * {@link #read()} are not idempotent, meaning the state might change between invocations.
 *
 * @param <T>: The type of elements in the stream.
 */
public interface CacheStreamProcessor<T> {

   /**
    * Defines the current element to process in the stream.
    *
    * @param element: Current element, might be <code>null</code>
    */
   void setCurrent(T element);

   /**
    * Identifies if the current processor is loaded with an element.
    *
    * @return <code>true</code>, if it has an element, <code>false</code> otherwise.
    */
   boolean hasElement();

   /**
    * Read the next byte available. As long as {@link #hasElement()} is <code>true</code>,
    * this method should be able to return something different of <code>-1</code>.
    *
    * @return the next byte or <code>-1</code> if nothing left from the current element.
    */
   byte read();
}
