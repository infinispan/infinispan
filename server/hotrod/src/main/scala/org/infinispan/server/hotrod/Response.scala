package org.infinispan.server.hotrod

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */

class Response(val opCode: OpCodes.OpCode,
               val id: Long,
               val status: Status.Status)