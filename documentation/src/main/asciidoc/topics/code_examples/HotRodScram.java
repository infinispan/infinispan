ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
clientBuilder.addServer()
               .host("127.0.0.1")
               .port(11222)
             .security()
               .authentication()
                 .saslMechanism("SCRAM-SHA-512")
                 .username("myuser")
                 .password("qwer1234!");
