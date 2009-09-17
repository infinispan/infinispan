package org.infinispan.rest


import apache.commons.httpclient.{HttpClient, HttpMethodBase}
import jboss.resteasy.plugins.server.servlet.{ResteasyBootstrap, HttpServletDispatcher}
import mortbay.jetty.servlet.Context

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
    ctx.addEventListener(new StartupListener)
    ctx.addServlet(classOf[HttpServletDispatcher], "/rest/*")
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