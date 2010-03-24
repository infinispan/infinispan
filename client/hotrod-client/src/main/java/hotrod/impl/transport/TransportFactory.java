package hotrod.impl.transport;

import hotrod.impl.Transport;
import hotrod.impl.transport.TcpTransport;

/**
 * // TODO: Document this
 *
 * @author mmarkus
 * @since 4.1
 */
public class TransportFactory {

   public Transport getTransport() {
      return new TcpTransport("a",1);
   }

}
