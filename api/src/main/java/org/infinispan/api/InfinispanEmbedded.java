package org.infinispan.api;

/**
 * Entry point for Infinispan API - client for embedded
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 10.0
 */
@Experimental
public class InfinispanEmbedded {
   public Infinispan newInfinispan(){
      throw new UnsupportedOperationException("embedded not supported yet");
   }
}
