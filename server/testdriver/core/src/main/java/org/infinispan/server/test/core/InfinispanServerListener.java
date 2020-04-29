package org.infinispan.server.test.core;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public interface InfinispanServerListener {

   default void before(InfinispanServerDriver driver) {
   }

   default void after(InfinispanServerDriver driver) {
   }
}
