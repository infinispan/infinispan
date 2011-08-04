/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.rest


import org.apache.commons.httpclient.{HttpClient, HttpMethodBase}
import org.jboss.resteasy.plugins.server.servlet.{ResteasyBootstrap, HttpServletDispatcher}
import org.mortbay.jetty.servlet.{ServletHolder, ServletHandler, Context}

/**
 * 
 * @author Michael Neale
 */

object ServerInstance {
  private val server = new org.mortbay.jetty.Server(8888);
  
  initServer()
  
  private def initServer() = {
    val ctx = new Context(server, "/", Context.SESSIONS)
    ctx.setInitParams(params)
    ctx.addEventListener(new ResteasyBootstrap)
    ctx.addServlet(classOf[HttpServletDispatcher], "/rest/*")
    
    val sh = new ServletHolder(classOf[StartupListener])
    sh setInitOrder 1
    sh.setInitParameter("infinispan.config", "test-config.xml")
    
    ctx.addServlet(sh, "/listener/*")
  }
  
  def started(): Boolean = {
    server.isStarted
  }

  def start(): Unit = {
    server.start
  }
  
  def stop(): Unit = {
    server.stop
  }

  private def params = {
    val hm = new java.util.HashMap[String, String]
    hm.put("resteasy.resources", "org.infinispan.rest.Server")
    hm
  }
}

object Client {
    val client = new HttpClient
    def call(method: HttpMethodBase) = {
      assert(ServerInstance.started)
      client.executeMethod(method)
      method
    }
}