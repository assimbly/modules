package org.assimbly.integrationrest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.assimbly.integrationrest.testcontainers.AssimblyGatewayHeadlessContainer;
import org.assimbly.integrationrest.utils.HttpUtil;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;

import java.net.http.HttpResponse;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

public class IntegrationRuntimeTest {

    @Test
    void shouldBeStarted() {
        try {
            // url
            String baseUrl = AssimblyGatewayHeadlessContainer.getBaseUrl();
            String url = baseUrl + "/api/integration/isstarted";

            // headers
            HashMap<String, String> headers = new HashMap();
            headers.put("Accept", MediaType.APPLICATION_JSON_VALUE);

            HttpResponse<String> response = HttpUtil.makeHttpCall(url, "GET", null, null, headers);

            assertThat(response.statusCode()).isEqualTo(HttpStatus.OK_200);

            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode expectedBodyJson = objectMapper.readTree("{\"path\":\"/integration/isstarted\",\"details\":\"successful\",\"id\":\"1\",\"message\":\"true\",\"status\":200}");

            assertThatJson(response.body())
                    .whenIgnoringPaths("timestamp")
                    .isEqualTo(expectedBodyJson);

        } catch (Exception e) {
            fail("Test failed due to unexpected exception: " + e.getMessage(), e);
        }
    }

}
