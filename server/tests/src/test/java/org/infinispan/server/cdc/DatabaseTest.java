package org.infinispan.server.cdc;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@ParameterizedTest
@ArgumentsSource(ChangeDataCaptureIT.DatabaseProvider.class)
@Target({ ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
public @interface DatabaseTest { }
