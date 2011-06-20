package org.jboss.seam.infinispan.test.testutil;

import org.jboss.seam.infinispan.Infinispan;
import org.jboss.seam.infinispan.event.cache.CacheEventBridge;
import org.jboss.seam.infinispan.event.cachemanager.CacheManagerEventBridge;
import org.jboss.shrinkwrap.api.GenericArchive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.DependencyResolvers;
import org.jboss.shrinkwrap.resolver.api.maven.MavenDependencyResolver;

public class Deployments {

	public static WebArchive baseDeployment() {
		return ShrinkWrap.create( WebArchive.class, "test.war" )
				.addPackage( Infinispan.class.getPackage() )
				.addPackage( CacheEventBridge.class.getPackage() )
				.addPackage( CacheManagerEventBridge.class.getPackage() )
				.addAsManifestResource( EmptyAsset.INSTANCE, "beans.xml" )
				.addAsLibraries(
						DependencyResolvers.use( MavenDependencyResolver.class )
								.artifact( "org.jboss.seam.solder:seam-solder:3.0.0.Final" )
								.resolveAs( GenericArchive.class )
				);
	}

}
