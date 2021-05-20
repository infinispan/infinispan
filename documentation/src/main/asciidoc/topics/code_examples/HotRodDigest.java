ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
clientBuilder.addServer()
               .host("127.0.0.1")
               .port(11222)
             .security()
               .authentication()
                 .saslMechanism("DIGEST-MD5")
                 .username("myuser")
                 .password("qwer1234!");
