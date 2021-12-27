package org.assimbly.connector.routes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.camel.*;
import org.apache.camel.api.management.ManagedCamelContext;
import org.apache.camel.api.management.mbean.ManagedCamelContextMBean;
import org.apache.camel.builder.DefaultErrorHandlerBuilder;
import org.apache.camel.builder.PredicateBuilder;
import org.apache.camel.builder.RouteBuilder;

import static org.apache.camel.language.groovy.GroovyLanguage.groovy;

import org.apache.camel.component.properties.PropertiesComponent;
import org.apache.commons.lang3.StringUtils;
import org.assimbly.connector.processors.ConvertProcessor;
import org.assimbly.connector.processors.FailureProcessor;
import org.assimbly.connector.processors.HeadersProcessor;
import org.assimbly.util.EncryptionUtil;
import org.jasypt.properties.EncryptableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConnectorRoute extends RouteBuilder {

	TreeMap<String, String> props;
	private DefaultErrorHandlerBuilder routeErrorHandler;
	private static Logger logger = LoggerFactory.getLogger("org.assimbly.connector.routes.DefaultRoute");
	private String flowId;
	private String flowName;
	private int maximumRedeliveries;
	private int redeliveryDelay;
	private int maximumRedeliveryDelay;
	private boolean parallelProcessing;
	private boolean assimblyHeaders;
	private int backOffMultiplier;

	private Processor headerProcessor;
	private Processor failureProcessor;
	private Processor convertProcessor;

	
	private List<String> onrampUriKeys;
	private List<String> offrampUriKeys;
	private List<String> responseUriKeys;
	private List<String> errorUriKeys;
	private String[] offrampUriList;

	int index = 0;
	private String logLevelAsString;

	public ConnectorRoute(final TreeMap<String, String> props){
		this.props = props;
	}

	public ConnectorRoute() {}

	public interface FailureProcessorListener {
		public void onFailure();
	}

	@Override
	public void configure() throws Exception {

		CamelContext context = getContext();
		ManagedCamelContext managed = context.getExtension(ManagedCamelContext.class);

		EncryptableProperties decryptedProperties = decryptProperties(props);

		headerProcessor = new HeadersProcessor(props);
		failureProcessor = new FailureProcessor(props);
		convertProcessor = new ConvertProcessor();

		flowId = props.get("id");
		errorUriKeys = getUriKeys("error");

		onrampUriKeys = getUriKeys("from");
		offrampUriKeys = getUriKeys("to");
		responseUriKeys = getUriKeys("response");
		offrampUriList = getOfframpUriList();

		setFlowSettings();

		setErrorHandler();
		
	
		//The default Camel route (onramp)
		for(String onrampUriKey : onrampUriKeys){

			String uri = props.get(onrampUriKey);
			uri = DecryptValue(uri);

			String endpointId = StringUtils.substringBetween(onrampUriKey, "from.", ".uri");
			String headerId = props.get("from." + endpointId + ".header.id");
			String routeId = props.get("from." + endpointId + ".route.id");

			Predicate hasParallelProcessing = PredicateBuilder.constant(parallelProcessing);
			Predicate hasAssimblyHeaders = PredicateBuilder.constant(assimblyHeaders);

			Predicate hasOneDestination = PredicateBuilder.constant(false);
			if(offrampUriList.length==1){
				hasOneDestination = PredicateBuilder.constant(true);
			}

			//this logic should be moved to a separate method getting config from service)
			if(uri.startsWith("rest")){
				String restHostAndPort = StringUtils.substringBetween(uri,"host=","&");
				if(restHostAndPort != null && !restHostAndPort.isEmpty()){
					String restHost = restHostAndPort.split(":")[0];
					String restPort = restHostAndPort.split(":")[1];
					restConfiguration().host(restHost).port(restPort).enableCORS(true);
				}
			}

			Predicate hasRoute = PredicateBuilder.constant(false);
			if(routeId!=null && !routeId.isEmpty()){
				hasRoute = PredicateBuilder.constant(true);
				String xml = props.get("from." + endpointId + ".route");
				addXmlRoute(xml, managed);
			}

			//The default Camel route (onramp)
			from(uri)
					.errorHandler(routeErrorHandler)
					.setHeader("AssimblyHeaderId", constant(headerId))
					.choice()
						.when(hasAssimblyHeaders)
							.setHeader("AssimblyFlowID", constant(flowId))
							.setHeader("AssimblyFrom", constant(props.get("from." + endpointId + ".uri")))
							.setHeader("AssimblyCorrelationId", simple("${date:now:yyyyMMdd}${exchangeId}"))
							.setHeader("AssimblyFromTimestamp", groovy("new Date().getTime()"))
					.end()
					.to("log:Flow=" + flowName + "|ID=" +  flowId + "|RECEIVED?level=" + logLevelAsString + "&showAll=true&multiline=true&style=Fixed")
					.process(headerProcessor)
					.id("headerProcessor" + flowId + "-" + endpointId)
					.process(convertProcessor)
					.id("convertProcessor" + flowId + "-" + endpointId)
					.choice()
						.when(hasRoute)
							.to("direct:flow=" + flowId + "route=" + flowId + "-" + endpointId + "-" + routeId)
					.end()
					.choice()
						.when(hasOneDestination)
							.to(offrampUriList)
						.endChoice()
						.when(hasParallelProcessing)
							.to(offrampUriList)
						.endChoice()
						.otherwise()
							.multicast()
							.shareUnitOfWork()
							.parallelProcessing()
							.to(offrampUriList)
					.end()
					.routeId(flowId + "-" + endpointId).description("from");
		}

		
		//The default Camel route (offramp)
		for (String offrampUriKey : offrampUriKeys)
		{

			String uri = props.get(offrampUriKey);
			uri = DecryptValue(uri);
			String offrampUri = offrampUriList[index++];
			String endpointId = StringUtils.substringBetween(offrampUriKey, "to.", ".uri");
			String headerId = props.get("to." + endpointId + ".header.id");
			String responseId = props.get("to." + endpointId + ".response.id");
			String routeId = props.get("to." + endpointId + ".route.id");

			Predicate hasAssimblyHeaders = PredicateBuilder.constant(assimblyHeaders);
			Predicate hasResponseEndpoint = PredicateBuilder.constant(responseId != null && !responseId.isEmpty());			
			Predicate hasRoute = PredicateBuilder.constant(false);

			boolean hasDynamicEndpoint = false;
			if(uri.contains("${")) {
				hasDynamicEndpoint = true;
			}
			
			if(routeId!=null && !routeId.isEmpty()){
				hasRoute = PredicateBuilder.constant(true);
				String xml = props.get("to." + endpointId + ".route");
				addXmlRoute(xml, managed);
			}

			if(hasDynamicEndpoint) {

				from(offrampUri)
				.errorHandler(routeErrorHandler)
				.to("log:Flow=" + flowName + "|ID=" +  flowId + "|SENDING?level=" + logLevelAsString + "&showAll=true&multiline=true&style=Fixed")
				.setHeader("AssimblyHeaderId", constant(headerId))
				.choice()
					.when(hasAssimblyHeaders)
						.setHeader("AssimblyTo", constant(uri))
						.setHeader("AssimblyToTimestamp", groovy("new Date().getTime()"))
				.end()
				.process(headerProcessor)
				.id("headerProcessor" + flowId + "-" + endpointId)
				.process(convertProcessor)
				.id("convertProcessor" + flowId + "-" + endpointId)
				.choice()
				.when(hasRoute)
				.to("direct:flow=" + flowId + "route=" + flowId + "-" + endpointId + "-" + routeId)
				.end()
				.log(hasResponseEndpoint.toString())
				.choice()
					.when(hasResponseEndpoint)
						.choice()
							.when(header("Enrich").convertToString().isEqualToIgnoreCase("to"))
								.to("log:Flow=" + flowName + "|ID=" +  flowId + "|ENRICH?level=" + logLevelAsString + "&showAll=true&multiline=true&style=Fixed")
								.pollEnrich().simple(uri).timeout(20000)
							.endChoice()
						.otherwise()
							.toD(uri)
							.to("log:Flow=" + flowName + "|ID=" +  flowId + "|SEND?level=" + logLevelAsString + "&showAll=true&multiline=true&style=Fixed")
							.to("direct:flow=" + flowId + "endpoint=" + responseId)
						.endChoice()
					.when(header("Enrich").convertToString().isEqualToIgnoreCase("to"))
						.to("log:Flow=" + flowName + "|ID=" +  flowId + "|ENRICH?level=" + logLevelAsString + "&showAll=true&multiline=true&style=Fixed")
						.pollEnrich().simple(uri).timeout(20000)
						.endChoice()
					.otherwise()
						.toD(uri)
						.to("log:Flow=" + flowName + "|ID=" +  flowId + "|SEND?level=" + logLevelAsString + "&showAll=true&multiline=true&style=Fixed")
					.end()
				.routeId(flowId + "-" + endpointId).description("to");
				
			}else {			

				from(offrampUri)
				.errorHandler(routeErrorHandler)
				.to("log:Flow=" + flowName + "|ID=" +  flowId + "|SENDING?level=" + logLevelAsString + "&showAll=true&multiline=true&style=Fixed")
				.setHeader("AssimblyHeaderId", constant(headerId))
				.choice()
					.when(hasAssimblyHeaders)
						.setHeader("AssimblyTo", constant(uri))
						.setHeader("AssimblyToTimestamp", groovy("new Date().getTime()"))
				.end()
				.process(headerProcessor)
				.id("headerProcessor" + flowId + "-" + endpointId)
				.process(convertProcessor)
				.id("convertProcessor" + flowId + "-" + endpointId)
				.choice()
				.when(hasRoute)
				.to("direct:flow=" + flowId + "route=" + flowId + "-" + endpointId + "-" + routeId)
				.end()
				.log(hasResponseEndpoint.toString())
				.choice()
					.when(hasResponseEndpoint)
						.choice()
							.when(header("Enrich").convertToString().isEqualToIgnoreCase("to"))
								.to("log:Flow=" + flowName + "|ID=" +  flowId + "|ENRICH?level=" + logLevelAsString + "&showAll=true&multiline=true&style=Fixed")
								.pollEnrich().simple(uri).timeout(20000)
							.endChoice()
						.otherwise()
							.to(uri)
							.to("log:Flow=" + flowName + "|ID=" +  flowId + "|SEND?level=" + logLevelAsString + "&showAll=true&multiline=true&style=Fixed")
							.to("direct:flow=" + flowId + "endpoint=" + responseId)
						.endChoice()
					.when(header("Enrich").convertToString().isEqualToIgnoreCase("to"))
						.to("log:Flow=" + flowName + "|ID=" +  flowId + "|ENRICH?level=" + logLevelAsString + "&showAll=true&multiline=true&style=Fixed")
						.pollEnrich().simple(uri).timeout(20000)
						.endChoice()
					.otherwise()
						.to(uri)
						.to("log:Flow=" + flowName + "|ID=" +  flowId + "|SEND?level=" + logLevelAsString + "&showAll=true&multiline=true&style=Fixed")
					.end()
				.routeId(flowId + "-" + endpointId).description("to");
				
			}
				
		}

		for(String responseUriKey : responseUriKeys){
			String uri = props.get(responseUriKey);
			String endpointId = StringUtils.substringBetween(responseUriKey, "response.", ".uri");
			String headerId = props.get("response." + endpointId + ".header.id");
			String responseId = props.get("response." + endpointId + ".response.id");
			String routeId = props.get("response." + endpointId + ".route.id");

			Predicate hasAssimblyHeaders = PredicateBuilder.constant(assimblyHeaders);
			Predicate hasRoute = PredicateBuilder.constant(false);

			if(routeId!=null && !routeId.isEmpty()){
				hasRoute = PredicateBuilder.constant(true);
				String xml = props.get("response." + endpointId + ".route");
				addXmlRoute(xml, managed);
			}

			from("direct:flow=" + flowId + "endpoint=" + responseId)
					.errorHandler(routeErrorHandler)
					.setHeader("AssimblyHeaderId", constant(headerId))
					.choice()
						.when(hasAssimblyHeaders)
							.setHeader("AssimblyFlowID", constant(flowId))
							.setHeader("AssimblyResponse", constant(props.get("response." + endpointId + ".uri")))
							.setHeader("AssimblyCorrelationId", simple("${date:now:yyyyMMdd}${exchangeId}"))
							.setHeader("AssimblyResponseTimestamp", groovy("new Date().getTime()"))
					.end()
					.process(headerProcessor)
					.id("headerProcessor" + flowId + "-" + endpointId)
					.process(convertProcessor)
					.id("convertProcessor" + flowId + "-" + endpointId)
					.choice()
						.when(hasRoute)
						.to("direct:flow=" + flowId + "route=" + flowId + "-" + endpointId + "-" + routeId)
					.end()
					.to("log:Flow=" + flowName + "|ID=" +  flowId + "|SENDINGRESPONSE?level=" + logLevelAsString + "&showAll=true&multiline=true&style=Fixed")
					.choice()
						.when(header("Enrich").convertToString().isEqualToIgnoreCase("response"))
							.to("log:Flow=" + flowName + "|ID=" +  flowId + "|ENRICH?level=" + logLevelAsString + "&showAll=true&multiline=true&style=Fixed")
							.pollEnrich().simple(uri).timeout(20000)
							.to("log:Flow=" + flowName + "|ID=" +  flowId + "|SENDRESPONSE?level=" + logLevelAsString + "&showAll=true&multiline=true&style=Fixed")
						.endChoice()
						.otherwise()
							.toD(uri)
						.to("log:Flow=" + flowName + "|ID=" +  flowId + "|SENDRESPONSE?level=" + logLevelAsString + "&showAll=true&multiline=true&style=Fixed")
					.end()
					.routeId(flowId + "-" + endpointId).description("response");
		}

	}

	
	private void setFlowSettings() {
		if (this.props.containsKey("flow.maximumRedeliveries")){
			String maximumRedeliveriesAsString = props.get("flow.maximumRedeliveries");
			if(StringUtils.isNumeric(maximumRedeliveriesAsString)) {
				maximumRedeliveries = Integer.parseInt(maximumRedeliveriesAsString);
			}else {
				maximumRedeliveries = 0;
			}
		}else {
			maximumRedeliveries = 0;
		}

		if (this.props.containsKey("flowredeliveryDelay")){
			String RedeliveryDelayAsString = props.get("flow.redeliveryDelay");
			if(StringUtils.isNumeric(RedeliveryDelayAsString)) {
				redeliveryDelay = Integer.parseInt(RedeliveryDelayAsString);
				maximumRedeliveryDelay = redeliveryDelay * 10;
			}else {
				redeliveryDelay = 3000;
				maximumRedeliveryDelay = 60000;
			}
		}else {
			redeliveryDelay = 3000;
			maximumRedeliveryDelay = 60000;
		}

		if (this.props.containsKey("flow.parallelProcessing")){
			String parallelProcessingAsString = props.get("flow.parallelProcessing");
			if(parallelProcessingAsString.equalsIgnoreCase("true")) {
				parallelProcessing = true;
			}else {
				parallelProcessing = false;
			}
		}else {
			parallelProcessing = true;
		}

		if (this.props.containsKey("flow.assimblyHeaders")){
			String assimblyHeadersAsString = props.get("flow.assimblyHeaders");
			if(assimblyHeadersAsString.equalsIgnoreCase("true")) {
				assimblyHeaders = true;
			}else {
				assimblyHeaders = false;
			}
		}else {
			assimblyHeaders = true;
		}

		if (this.props.containsKey("flow.backOffMultiplier")){
			String backOffMultiplierAsString = props.get("flow.backOffMultiplier");
			if(StringUtils.isNumeric(backOffMultiplierAsString)) {
				backOffMultiplier = Integer.parseInt(backOffMultiplierAsString);
			}else {
				backOffMultiplier = 0;
			}
		}else {
			backOffMultiplier = 0;
		}

		flowName = props.get("flow.name");
		if (this.props.containsKey("flow.logLevel")){
			logLevelAsString = props.get("flow.logLevel");
		}else {
			logLevelAsString = "OFF";
		}

	}
	
	private void setErrorHandler() {

		if (this.props.containsKey(errorUriKeys.get(0))){
			routeErrorHandler = deadLetterChannel(props.get(errorUriKeys.get(0)))
					.allowRedeliveryWhileStopping(false)
					.asyncDelayedRedelivery()
					.maximumRedeliveries(maximumRedeliveries)
					.redeliveryDelay(redeliveryDelay)
					.maximumRedeliveryDelay(maximumRedeliveryDelay)
					.backOffMultiplier(backOffMultiplier)
					.retriesExhaustedLogLevel(LoggingLevel.ERROR)
					.retryAttemptedLogLevel(LoggingLevel.DEBUG)
					.onExceptionOccurred(failureProcessor)
					.log(log)
					.logRetryStackTrace(false)
					.logStackTrace(true)
					.logHandled(true)
					.logExhausted(true)
					.logExhaustedMessageHistory(true);
		}
		else{
			routeErrorHandler = defaultErrorHandler()
					.allowRedeliveryWhileStopping(false)
					.asyncDelayedRedelivery()
					.maximumRedeliveries(maximumRedeliveries)
					.redeliveryDelay(redeliveryDelay)
					.maximumRedeliveryDelay(maximumRedeliveryDelay)
					.backOffMultiplier(backOffMultiplier)
					.retriesExhaustedLogLevel(LoggingLevel.ERROR)
					.retryAttemptedLogLevel(LoggingLevel.DEBUG)
					.onExceptionOccurred(failureProcessor)
					.logRetryStackTrace(false)
					.logStackTrace(true)
					.logHandled(true)
					.logExhausted(true)
					.logExhaustedMessageHistory(true)
					.log(logger);
		}

		routeErrorHandler.setAsyncDelayedRedelivery(true);

	}
	
	
	//create a string array for all offramps
	private String[] getOfframpUriList() {

		String offrampUri = props.get("offramp.uri.list");

		return offrampUri.split(",");

	}

	//create a string array for all of a specific endpointType
	private List<String> getUriKeys(String endpointType) {

		List<String> keys = new ArrayList<>();

		for(String prop : props.keySet()){
			if(prop.startsWith(endpointType) && prop.endsWith("uri")){
				keys.add(prop);
			}
		}

		return keys;

	}

	private void addXmlRoute(String xml, ManagedCamelContext managed) throws Exception {
		ManagedCamelContextMBean managedContext = managed.getManagedCamelContext();
		managedContext.addOrUpdateRoutesFromXml(xml);
	}

	//
	private EncryptableProperties decryptProperties(TreeMap<String, String> properties) {
		EncryptableProperties decryptedProperties = (EncryptableProperties) ((PropertiesComponent) getContext().getPropertiesComponent()).getInitialProperties();
		decryptedProperties.putAll(properties);
		return decryptedProperties;
	}

	private String DecryptValue(String value){

		EncryptableProperties encryptionProperties = (EncryptableProperties) ((PropertiesComponent) getContext().getPropertiesComponent()).getInitialProperties();
		String[] encryptedList = StringUtils.substringsBetween(value, "ENC(", ")");

		if(encryptedList !=null && encryptedList.length>0){
			for (String encrypted: encryptedList) {
				encryptionProperties.setProperty("temp","ENC(" + encrypted + ")");
				String decrypted = encryptionProperties.getProperty("temp");
				value = StringUtils.replace(value, "ENC(" + encrypted + ")",decrypted);
			}
		}

		return value;

	}

}