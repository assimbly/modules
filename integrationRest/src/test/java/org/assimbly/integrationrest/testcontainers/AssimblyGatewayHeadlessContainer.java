package org.assimbly.integrationrest.testcontainers;

import org.assimbly.integrationrest.AuthenticationRuntimeTest;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class AssimblyGatewayHeadlessContainer {

    private static final GenericContainer<?> container;
    private static String tokenId;

    static {
        container = new GenericContainer<>("assimbly/gateway-headless:development")
                .withExposedPorts(8088)
                .waitingFor(Wait.forLogMessage(".*Assimbly is running!.*", 1));
        container.start();
    }

    public static GenericContainer<?> getInstance() {
        return container;
    }

    public static String getBaseUrl() {
        String host = container.getHost();
        Integer port = container.getMappedPort(8088);
        return "http://" + host + ":" + port;
    }

    public static String getTokenId() {
        if(tokenId == null) {
            tokenId = AuthenticationRuntimeTest.getTokenId();
        }
        return tokenId;
    }
}
