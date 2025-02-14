package org.assimbly.integrationrest;

import org.assimbly.integrationrest.utils.CamelContextUtil;
import org.assimbly.integrationrest.testcontainers.AssimblyGatewayHeadlessContainer;
import org.assimbly.integrationrest.utils.HttpUtil;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;

import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class FlowManagerRuntimeTest {

    private Properties camelContextProp = CamelContextUtil.buildExample();

    @Test
    void shouldInstallFlow() {
        try {
            // url
            String baseUrl = AssimblyGatewayHeadlessContainer.getBaseUrl();
            String url = String.format("%s/api/integration/flow/%s/install", baseUrl, camelContextProp.get(CamelContextUtil.Field.id.name()));

            // headers
            HashMap<String, String> headers = new HashMap();
            headers.put("Accept", MediaType.APPLICATION_JSON_VALUE);
//            headers.put("Authorization", String.format("Bearer %s", AssimblyGatewayHeadlessContainer.getTokenId()));
            headers.put("charset", StandardCharsets.ISO_8859_1.displayName());
            headers.put("Content-type", MediaType.APPLICATION_XML_VALUE);

            HttpResponse<String> response = HttpUtil.makeHttpCall(url, "POST", (String)camelContextProp.get(CamelContextUtil.Field.camelContext.name()), null, headers);

            assertThat(response.statusCode()).isEqualTo(HttpStatus.OK_200);

        } catch (Exception e) {
            fail("Test failed due to unexpected exception: " + e.getMessage(), e);
        }
    }

}
