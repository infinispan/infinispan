package org.infinispan.client.rest;

import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;

public class McpProxy {
   public static void main(String[] args) {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();

      RestClient client = RestClient.forConfiguration(builder.build());
      for(String line = System.console().readLine(); line != null; line = System.console().readLine()) {
         System.out.println(line);
      }
   }
}
