package org.infinispan.server.hotrod

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */

class ErrorResponse(override val opCode: OpCodes.OpCode,
                    override val id: Long,
                    override val status: Status.Status,
                    val msg: String) extends Response(opCode, id, status)