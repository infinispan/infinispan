package test.infinispan.integration.embedded;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.boot.resttestclient.TestRestTemplate;


@SpringBootTest(webEnvironment= SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.main.banner-mode=off")
@AutoConfigureTestRestTemplate
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
        String body = response.getBody();
        assertThat(body).contains("cache_puts_latency_MILLISECONDS{cache=\"simpleCache\"");
        assertThat(body).contains("cache_puts_nlatency_NANOSECONDS{cache=\"simpleCache\"");
        assertThat(body).contains("cache_removes_latency_MILLISECONDS{cache=\"simpleCache\"");
        assertThat(body).contains("cache_removes_nlatency_NANOSECONDS{cache=\"simpleCache\"");
        assertThat(body).contains("cache_start_SECONDS{cache=\"simpleCache\"");
        assertThat(body).contains("cache_reset_SECONDS{cache=\"simpleCache\"");
        assertThat(body).contains("cache_gets_total{cache=\"simpleCache\"");
        assertThat(body).contains("cache_size{cache=\"simpleCache\"");
        assertThat(body).contains("cache_evictions_total{cache=\"simpleCache\"");
    }
}
