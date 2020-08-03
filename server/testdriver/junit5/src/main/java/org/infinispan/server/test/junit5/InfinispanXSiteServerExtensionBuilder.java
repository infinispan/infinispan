package org.infinispan.server.test.junit5;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.server.test.core.TestServer;

/**
 * Infinispan Server XSite Extension Builder
 *
 * @author Gustavo Lira
 * @since 12.0
 */
public class InfinispanXSiteServerExtensionBuilder {

   private final List<InfinispanServerExtensionBuilder> sites = new ArrayList<>();

   public InfinispanXSiteServerExtensionBuilder addSite(String siteName, InfinispanServerExtensionBuilder siteBuilder) {
      siteBuilder.site(siteName);
      sites.add(siteBuilder);
      return this;
   }

   public InfinispanXSiteServerExtension build() {
      Set<String> uniqueSiteName = new HashSet<>();
      List<TestServer> testServers = sites.stream()
            .map(it -> new TestServer(it.createServerTestConfiguration()))
            .peek(testServer -> {
               if (!uniqueSiteName.add(testServer.getSiteName())) {
                  throw new IllegalStateException("Site name already set: " + testServer.getSiteName());
               }
            })
            .collect(Collectors.toList());

      return new InfinispanXSiteServerExtension(testServers);
   }
}
