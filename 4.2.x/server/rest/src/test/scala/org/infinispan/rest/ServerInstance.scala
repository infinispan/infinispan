package org.infinispan.rest


import org.apache.commons.httpclient.{HttpClient, HttpMethodBase}
import org.jboss.resteasy.plugins.server.servlet.{ResteasyBootstrap, HttpServletDispatcher}
import org.mortbay.jetty.servlet.{ServletHolder, ServletHandler, Context}

/**
 * 
 * @author Michael Neale
 */

object ServerInstance {
  val server = startServer


  def startServer = {
    val server = new org.mortbay.jetty.Server(8888);
    val ctx = new Context(server, "/", Context.SESSIONS)
    ctx.setInitParams(params)
    ctx.addEventListener(new ResteasyBootstrap)
    ctx.addServlet(classOf[HttpServletDispatcher], "/rest/*")
    val sh = new ServletHolder(classOf[StartupListener])
    sh setInitOrder 1
    ctx.addServlet(sh, "/listener/*")
    server.setStopAtShutdown(true)
    server.start
    server
  }

  def params = {
    val hm = new java.util.HashMap[String, String]
    hm.put("resteasy.resources", "org.infinispan.rest.Server")
    hm
  }



}

object Client {
    val client = new HttpClient
    def call(method: HttpMethodBase) = {
      val s = ServerInstance.server
      client.executeMethod(method)
      method
    }
}