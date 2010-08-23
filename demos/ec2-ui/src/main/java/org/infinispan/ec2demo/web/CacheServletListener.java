package org.infinispan.ec2demo.web;

import org.infinispan.ec2demo.CacheBuilder;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.io.IOException;

/**
 * @author noconnor@redhat.com
 *
 */
public class CacheServletListener implements ServletContextListener {
	private CacheBuilder currCacheBuilder;
	private ServletContext context = null;

	/* (non-Javadoc)
	 * @see javax.servlet.ServletContextListener#contextDestroyed(javax.servlet.ServletContextEvent)
	 */
	@Override
	public void contextDestroyed(ServletContextEvent arg0) {
		currCacheBuilder.getCacheManager().stop();
	}

	/* (non-Javadoc)
	 * @see javax.servlet.ServletContextListener#contextInitialized(javax.servlet.ServletContextEvent)
	 */
	@Override
	public void contextInitialized(ServletContextEvent arg0) {
		System.out.println("in CacheServletListener");
		try {
			this.context = arg0.getServletContext();
			String x = arg0.getServletContext().getInitParameter("InfinispanConfigFile");
			currCacheBuilder =  new CacheBuilder(x);
			System.out.println("in CacheServletListener...starting cache");
			currCacheBuilder.getCacheManager().start();
			System.out.println("in CacheServletListener...starting cache...done");
			context.setAttribute("cacheBuilder", currCacheBuilder);
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("in CacheServletListener...exit");
	}

}
