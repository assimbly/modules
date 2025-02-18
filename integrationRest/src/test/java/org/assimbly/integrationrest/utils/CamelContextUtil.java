package org.assimbly.integrationrest.utils;

import java.util.Properties;

public class CamelContextUtil {

    public enum Field {
        id,
        routeId1,
        routeId2,
        routeId3,
        camelContext
    }

    static public Properties buildExample() {
        Properties props = new Properties();

        StringBuffer camelContextBuf = new StringBuffer();

        camelContextBuf.append("<camelContext xmlns=\"http://camel.apache.org/schema/blueprint\" id=\"67921474ecaafe0007000000\" useMDCLogging=\"true\" streamCache=\"true\">")
        .append("<jmxAgent id=\"agent\" loadStatisticsEnabled=\"true\" />")
        .append("<streamCaching id=\"streamCacheConfig\" spoolThreshold=\"0\" spoolDirectory=\"tmp/camelcontext-#camelId#\" spoolUsedHeapMemoryThreshold=\"70\" />")
        .append("<threadPoolProfile id=\"wiretapProfile\" defaultProfile=\"false\" poolSize=\"0\" maxPoolSize=\"5\" maxQueueSize=\"2000\" rejectedPolicy=\"DiscardOldest\" keepAliveTime=\"10\" />")
        .append("<threadPoolProfile id=\"defaultProfile\" defaultProfile=\"true\" poolSize=\"0\" maxPoolSize=\"10\" maxQueueSize=\"1000\" rejectedPolicy=\"CallerRuns\" keepAliveTime=\"30\" />")
        .append("<onException>")
        .append("<exception>java.lang.Exception</exception>")
        .append("<redeliveryPolicy maximumRedeliveries=\"0\" redeliveryDelay=\"5000\" />")
        .append("<setExchangePattern pattern=\"InOnly\" />")
        .append("</onException>")
        .append("<route id=\"3d01e43c-6e86-4c9e-9972-7c872ecc37f6\">")
        .append("<from uri=\"jetty:https://0.0.0.0:9001/dovetail/SimpleTest?httpBinding=#customHttpBinding&amp;matchOnUriPrefix=false&amp;continuationTimeout=0&amp;sslContextParameters=sslContext\" />")
        .append("<removeHeaders pattern=\"CamelHttp*\" />")
        .append("<to uri=\"direct:67921474ecaafe0007000000_test_3d01e43c-6e86-4c9e-9972-7c872ecc37f6?exchangePattern=InOut\" />")
        .append("</route>")
        .append("<route id=\"0e2208f0-3a58-4a9f-a0ae-41a66f184282\">")
        .append("<from uri=\"direct:67921474ecaafe0007000000_test_3d01e43c-6e86-4c9e-9972-7c872ecc37f6\" />")
        .append("<setHeader headerName=\"test\">")
        .append("<constant>test header content</constant>")
        .append("</setHeader>")
        .append("<to uri=\"direct:67921474ecaafe0007000000_test_0e2208f0-3a58-4a9f-a0ae-41a66f184282\" />")
        .append("</route>")
        .append("<route id=\"44ac76a8-a1d1-4b1d-a93c-1c9ce4c615e9\">")
        .append("<from uri=\"direct:67921474ecaafe0007000000_test_0e2208f0-3a58-4a9f-a0ae-41a66f184282\" />")
        .append("<setBody>")
        .append("<simple>${header.test}</simple>")
        .append("</setBody>")
        .append("</route>")
        .append("<property key=\"frontend.engine\" value=\"dovetail\" />")
        .append("</camelContext>");

        props.setProperty(Field.id.name(), "67921474ecaafe0007000000");
        props.setProperty(Field.routeId1.name(), "3d01e43c-6e86-4c9e-9972-7c872ecc37f6");
        props.setProperty(Field.routeId2.name(), "0e2208f0-3a58-4a9f-a0ae-41a66f184282");
        props.setProperty(Field.routeId3.name(), "44ac76a8-a1d1-4b1d-a93c-1c9ce4c615e9");
        props.setProperty(Field.camelContext.name(), camelContextBuf.toString());

        return props;
    }
}
