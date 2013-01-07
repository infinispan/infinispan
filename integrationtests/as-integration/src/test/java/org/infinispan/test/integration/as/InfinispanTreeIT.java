/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.test.integration.as;

import org.infinispan.Cache;
import org.infinispan.Version;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.tree.Fqn;
import org.infinispan.tree.Node;
import org.infinispan.tree.TreeCache;
import org.infinispan.tree.TreeCacheFactory;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.Asset;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.descriptor.api.Descriptors;
import org.jboss.shrinkwrap.descriptor.api.spec.se.manifest.ManifestDescriptor;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Test the Infinispan AS module integration
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@RunWith(Arquillian.class)
public class InfinispanTreeIT {

   @Deployment
   public static Archive<?> deployment() {
      WebArchive archive = ShrinkWrap.create(WebArchive.class, "simple.war").addClass(InfinispanTreeIT.class).add(manifest(), "META-INF/MANIFEST.MF");
      return archive;
   }

   private static Asset manifest() {
      String manifest = Descriptors.create(ManifestDescriptor.class)
            .attribute("Dependencies", "org.infinispan:" + Version.MAJOR_MINOR + " service, org.infinispan.tree:" + Version.MAJOR_MINOR + " services").exportAsString();
      return new StringAsset(manifest);
   }

   @Test
   public void testCacheManager() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.transaction().invocationBatching().enable();
      EmbeddedCacheManager cm = new DefaultCacheManager(builder.build());
      Cache<String, String> cache = cm.getCache();
      TreeCacheFactory tcf = new TreeCacheFactory();
      TreeCache<String, String> tree = tcf.createTreeCache(cache);
      Fqn leafFqn = Fqn.fromElements("branch", "leaf");
      Node<String, String> leaf = tree.getRoot().addChild(leafFqn);
      leaf.put("fruit", "orange");
      cm.stop();
   }

}
