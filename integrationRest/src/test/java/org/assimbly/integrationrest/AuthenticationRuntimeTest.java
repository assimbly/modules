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

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class AuthenticationRuntimeTest {

    private static String ID_TOKEN = "id_token";

    public static String getTokenId() {
        try {
            // url
            String baseUrl = AssimblyGatewayHeadlessContainer.getBaseUrl();
            String url = String.format("%s/api/authenticate", baseUrl);

            // headers
            HashMap<String, String> headers = new HashMap();
            headers.put("Content-type", MediaType.APPLICATION_JSON_VALUE);
            headers.put("db", "mongo");

            // body
            String body = "{\"username\": \"admin\", \"password\": \"admin\", \"rememberMe\": \"false\"}";

            HttpResponse<String> response = HttpUtil.makeHttpCall(url, "POST", body, null, headers);

            if(response.statusCode() != HttpStatus.OK_200) {
                return null;
            }

            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode bodyJson = objectMapper.readTree(response.body());

            if(bodyJson.get(ID_TOKEN) != null) {
                return bodyJson.get(ID_TOKEN).textValue();
            }

            return null;

        } catch (Exception e) {
            return null;
        }
    }

    @Test
    void shouldAuthenticateAndGetToken() {
        try {
            // url
            String baseUrl = AssimblyGatewayHeadlessContainer.getBaseUrl();
            String url = String.format("%s/api/authenticate", baseUrl);

            // headers
            HashMap<String, String> headers = new HashMap();
            headers.put("Content-type", MediaType.APPLICATION_JSON_VALUE);
            headers.put("db", "mongo");

            // body
            String body = "{\"username\": \"admin\", \"password\": \"admin\", \"rememberMe\": \"false\"}";

            HttpResponse<String> response = HttpUtil.makeHttpCall(url, "POST", body, null, headers);

            assertThat(response.statusCode()).isEqualTo(HttpStatus.OK_200);
            assertThatJson(response.body()).inPath(ID_TOKEN);

        } catch (Exception e) {
            fail("Test failed due to unexpected exception: " + e.getMessage(), e);
        }
    }

}
