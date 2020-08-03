package org.infinispan.server.test.junit4;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.server.test.core.TestServer;

/**
 * Builder for {@link InfinispanXSiteServerRule}.
 *
 * @author Gustavo Lira &lt;gliraesi@redhat.com&gt;
 * @since 12.0
 */

public class InfinispanXSiteServerRuleBuilder {

   private final List<InfinispanServerRuleBuilder> sites = new ArrayList<>();

   public InfinispanXSiteServerRuleBuilder addSite(String siteName, InfinispanServerRuleBuilder siteBuilder) {
      siteBuilder.site(siteName);
      sites.add(siteBuilder);
      return this;
   }

   public InfinispanXSiteServerRule build() {
      Set<String> uniqueSiteName = new HashSet<>();
      List<TestServer> sitesTestServers = sites.stream()
            .map(InfinispanServerRuleBuilder::build)
            .map(InfinispanServerRule::getTestServer)
            .peek(testServer -> {
               if (!uniqueSiteName.add(testServer.getSiteName())) {
                  throw new IllegalStateException("Site name already set: " + testServer.getSiteName());
               }
            })
            .collect(Collectors.toList());
      return new InfinispanXSiteServerRule(sitesTestServers);
   }

}
