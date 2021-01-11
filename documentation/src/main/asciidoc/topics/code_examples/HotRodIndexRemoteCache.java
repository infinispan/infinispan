ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServer()
               .host("127.0.0.1")
               .port(ConfigurationProperties.DEFAULT_HOTROD_PORT)
             .security().authentication()
               .username("username")
               .password("password")
               .realm("default")
               .saslMechanism("DIGEST-MD5");
      builder.remoteCache("my-cache")
               .configuration("<infinispan><cache-container><distributed-cache name=\"my-cache\"><encoding media-type=\"application/x-protostream\"/><indexing><indexed-entities><indexed-entity>book_sample.Book</indexed-entity></indexed-entities></indexing></distributed-cache></cache-container></infinispan>");
