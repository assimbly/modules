package org.assimbly.dil.event.collect;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.builder.ExpressionBuilder;
import org.apache.camel.spi.CamelEvent;
import org.apache.camel.spi.UnitOfWork;
import org.apache.camel.support.EventNotifierSupport;
import org.assimbly.dil.event.domain.Filter;
import org.assimbly.dil.event.domain.Store;
import org.assimbly.dil.event.store.StoreManager;
import org.assimbly.dil.event.util.EventUtil;
import org.assimbly.dil.event.domain.MessageEvent;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

//Check following page for all Event instances: https://www.javadoc.io/doc/org.apache.camel/camel-api/latest/org/apache/camel/spi/CamelEvent.html

public class MessageCollector extends EventNotifierSupport {

    private final StoreManager storeManager;
    private final String expiryInHours;
    private final ArrayList<Filter> filters;
    private final ArrayList<String> events;
    private final String collectorId;
    private final String flowId;
    private final String flowVersion;


    public MessageCollector(String collectorId, String flowId, String flowVersion, ArrayList<String> events, ArrayList<Filter> filters, ArrayList<org.assimbly.dil.event.domain.Store> stores) {
        this.collectorId = collectorId;
        this.flowId = flowId;
        this.flowVersion = flowVersion;
        this.events = events;
        this.filters = filters;
        this.storeManager = new StoreManager(collectorId, stores);
        List<Store> elasticStores = stores.stream().filter(p -> p.getType().equals("elastic")).collect(Collectors.toList());
        if(elasticStores.size()==1){
            this.expiryInHours = elasticStores.get(0).getExpiryInHours();
        }else{
            this.expiryInHours = "1";
        }
    }


    @Override
    public void notify(CamelEvent event) throws Exception {

        String type = event.getType().name();

        //filter only the configured events
        if(events!=null && events.contains(type)) {

            // Cast to exchange event
            CamelEvent.ExchangeEvent exchangeEvent = (CamelEvent.ExchangeEvent) event;

            // Get the message exchange from exchange event
            Exchange exchange = exchangeEvent.getExchange();

            // Get the stepid
            String stepId = exchange.getFromRouteId();

            //Set default headers for the response time
            long created = exchange.getCreated();

            System.out.println("Created=" + created);

            if(created!=0){
                Object initTime = exchange.getIn().getHeader("ComponentInitTime", Long.class);
                exchange.getIn().setHeader("ComponentInitTime", created);
                if(initTime != null) {
                    long duration = created - (long)initTime;
                    System.out.println("Duration=" + duration);
                    exchange.getIn().setHeader("ComponentResponseTime", Long.toString(duration));
                }
            }

            //process and store the exchange
            if (stepId != null && stepId.startsWith(flowId) && filters == null) {
                processEvent(exchange, flowId, stepId);
            } else if (stepId != null && stepId.startsWith(flowId) && EventUtil.isFiltered(filters, stepId)) {
                processEvent(exchange, flowId, stepId);
            }

        }

    }

    private void processEvent(Exchange exchange, String flowId, String stepId){

        //set fields
        Message message = exchange.getMessage();
        String body = getBody(message);
        Map<String, Object> headers = message.getHeaders();
        String messageId = message.getMessageId();

        //use breadcrumbId when available
        messageId = message.getHeader("breadcrumbId", messageId, String.class);

        String timestamp = EventUtil.getTimestamp();
        String expiryDate = EventUtil.getExpiryTimestamp(expiryInHours);

        //create json
        MessageEvent messageEvent = new MessageEvent(timestamp, messageId, flowId, flowVersion, stepId, headers, body, expiryDate);
        String json = messageEvent.toJson();

        //store the event
        storeManager.storeEvent(json);
    }

    public String getBody(Message message) {

        try {

            byte[] body = message.getBody(byte[].class);

            if (body == null || body.length == 0) {
                return "<empty>";
            }else if (body.length > 250000) {
                return new String(Arrays.copyOfRange(body, 0, 250000), StandardCharsets.UTF_8);
            }else{
                return new String (body, StandardCharsets.UTF_8);
            }

        } catch (Exception e) {
            String typeName = message.getBody().getClass().getTypeName();
            if(typeName!= null && !typeName.isEmpty()){
                return "<" + typeName + ">";
            }else{
                return "<unable to convert>";
            }
        }

    }

}
