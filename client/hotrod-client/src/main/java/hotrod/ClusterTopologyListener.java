package hotrod;

import java.util.List;

/**
 * // TODO: Document this
 *
 * @author mmarkus
 * @since 4.1
 */
public interface ClusterTopologyListener {

   //todo consider using inet address
   public class Address {
      private String host;
      private int port;
   }

   public void nodeAdded(List<Address> currentTopology, Address addedNode);

   public void  nodeRemoved(List<Address> currentTopology, Address removedNode);
}
