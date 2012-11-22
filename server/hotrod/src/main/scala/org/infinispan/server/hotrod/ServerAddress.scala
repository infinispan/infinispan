/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.server.hotrod

/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

import org.infinispan.remoting.transport.Address
import java.nio.charset.Charset
import java.util.Arrays
import org.infinispan.marshall.AbstractExternalizer
import java.io.{ObjectInput, ObjectOutput}
import scala.collection.JavaConversions._

/**
 * A Hot Rod server address
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
class ServerAddress(val host: String, val port: Int) extends Address {

   import ServerAddress._

//   // IMPORTANT NOTE: Hot Rod protocol agrees to this calculation for a node
//   // address hash code calculation, so any changes to the implementation
//   // require modification of the protocol.
//   override def hashCode() = Arrays.hashCode(
//      "%s:%d".format(host, port).getBytes(UTF8))

   override def hashCode() = (31 * host.hashCode()) + port

   override def equals(obj: Any): Boolean = {
      obj match {
         case s: ServerAddress => s.host == host && s.port == port
         case _ => false
      }
   }

   override def toString = "%s:%d".format(host, port)

   def compareTo(o: Address) : Int = {
      o match {
         case oa : ServerAddress => {
            var cmp = host.compareTo(oa.host)
            if (cmp == 0) {
               cmp = port - oa.port
            }
            cmp
         }
         case _ => 0
      }
   }
}

object ServerAddress {

//   val UTF8 = Charset.forName("UTF-8")

   class Externalizer extends AbstractExternalizer[ServerAddress] {

      def writeObject(out: ObjectOutput, obj: ServerAddress) {
         out.writeObject(obj.host)
         out.writeShort(obj.port)
      }

      def readObject(in: ObjectInput): ServerAddress = {
         val host = in.readObject.asInstanceOf[String]
         val port = in.readUnsignedShort()
         new ServerAddress(host, port)
      }

      def getTypeClasses = setAsJavaSet(
         Set[java.lang.Class[_ <: ServerAddress]](classOf[ServerAddress]))

   }

}
