ConfigurationBuilder remoteBuilder = new ConfigurationBuilder();
remoteBuilder.addServer().host(host).port(Integer.parseInt(port));

// Add your generated SerializationContextInitializer implementation.
LibraryInitalizer initializer = new LibraryInitalizerImpl();
remoteBuilder.addContextInitializer(initializer);
