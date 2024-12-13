package test.infinispan.integration.embedded;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment= SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.main.banner-mode=off")
@AutoConfigureObservability
public class SimpleApplicationTests {
    @LocalServerPort
    protected int port;

    @Autowired
    protected TestRestTemplate testRestTemplate;

    @Test
    void contextLoads() {

        ResponseEntity<String> response = testRestTemplate
                .withBasicAuth("user", "password")
                .getForEntity("http://localhost:" + port + "/actuator/prometheus", String.class);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertTrue(response.getBody().contains("cache_puts_latency_MILLISECONDS{cache=\"simpleCache\""));
        assertTrue(response.getBody().contains("cache_puts_nlatency_NANOSECONDS{cache=\"simpleCache\""));
        assertTrue(response.getBody().contains("cache_removes_latency_MILLISECONDS{cache=\"simpleCache\""));
        assertTrue(response.getBody().contains("cache_removes_nlatency_NANOSECONDS{cache=\"simpleCache\""));
        assertTrue(response.getBody().contains("cache_start_SECONDS{cache=\"simpleCache\""));
        assertTrue(response.getBody().contains("cache_reset_SECONDS{cache=\"simpleCache\""));
        assertTrue(response.getBody().contains("cache_gets_total{cache=\"simpleCache\""));
        assertTrue(response.getBody().contains("cache_size{cache=\"simpleCache\""));
        assertTrue(response.getBody().contains("cache_evictions_total{cache=\"simpleCache\""));
    }
}
