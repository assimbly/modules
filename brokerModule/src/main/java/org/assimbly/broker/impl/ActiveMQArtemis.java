package org.assimbly.broker.impl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Arrays.stream;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.management.impl.ActiveMQServerControlImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.commons.io.FileUtils;
import org.assimbly.broker.Broker;
import org.assimbly.broker.converter.CompositeDataConverter;
import org.assimbly.util.BaseDirectory;
import org.assimbly.util.ConnectorUtil;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.openmbean.CompositeData;

public class ActiveMQArtemis implements Broker {

	private static Logger logger = LoggerFactory.getLogger("org.assimbly.broker.impl.ActiveMQArtemis");

	EmbeddedActiveMQ broker;
    private final String baseDir = BaseDirectory.getInstance().getBaseDirectory();

	File brokerFile = new File(baseDir + "/broker/broker.xml");
	private ActiveMQServerControlImpl manageBroker;
	private boolean endpointExist;

	public void setBaseDirectory(String baseDirectory) {
		BaseDirectory.getInstance().setBaseDirectory(baseDirectory);
	}

	//See docs https://activemq.apache.org/components/artemis/documentation/javadocs/javadoc-latest/org/apache/activemq/artemis/api/core/management/QueueControl.html

	public String start() throws Exception {

		broker = new EmbeddedActiveMQ();

		if(brokerFile.exists()) {
			String fileConfig = "file:///" + brokerFile.getAbsolutePath();
			logger.info("Using config file 'broker.xml'. Loaded from " + brokerFile.getAbsolutePath());
			logger.info("broker.xml documentation reference: https://activemq.apache.org/components/artemis/documentation/latest/configuration-index.html");
			broker.setConfigResourcePath(fileConfig);
		}else {
			
			this.setFileConfiguration("");
			logger.warn("No config file 'broker.xml' found.");
			logger.info("Created default 'broker.xml' stored in following directory: " + baseDir + "/broker");			
			logger.info("broker.xml documentation reference: https://activemq.apache.org/components/artemis/documentation/latest/configuration-index.html");
			logger.info("");
			logger.info("Start broker in local mode on url: tcp://127.0.0.1:61616");
			
			String fileConfig = "file:///" + brokerFile.getAbsolutePath();
			broker.setConfigResourcePath(fileConfig);
		}		
		
		broker.start();

		setManageBroker();

		return status();
	}


	public String startEmbedded() throws Exception {

			logger.warn("Start embedded broker in local mode on url: tcp://127.0.0.1:61616");

			Configuration config = new ConfigurationImpl();
			config.addAcceptorConfiguration("in-vm", "vm://0");
			config.addAcceptorConfiguration("tcp", "tcp://127.0.0.1:61616");
			config.setSecurityEnabled(false);

			broker = new EmbeddedActiveMQ();
			broker.setConfiguration(config);
			broker.start();

			return status();
	}

	
	public String stop() throws Exception {
		ActiveMQServer activeBroker = broker.getActiveMQServer();
		
		if(activeBroker!=null) {
			SimpleString nodeID= activeBroker.getNodeID();
			logger.info("Broker with nodeId '" + nodeID + "' is stopping. Uptime=" + activeBroker.getUptime());
			broker.stop();
			logger.info("Broker with nodeId '" + nodeID + "' is stopped.");
		}
		
		return status();
		
	}

	public String restart() throws Exception {
		this.stop();
		this.start();
		
		return status();
	}

	
	public String restartEmbedded() throws Exception {
		this.stop();
		this.startEmbedded();
		
		return status();
		
	}	
	
	public String status() throws Exception {
		String status = "stopped";
		if(broker==null) {
			broker = new EmbeddedActiveMQ();
		}
		ActiveMQServer activeBroker = broker.getActiveMQServer();
		if(activeBroker!=null) {
			if(activeBroker.isActive()) {
				status = "started";	
			}		
		}		
		return status;
	}

	public String info() throws Exception {
		
		if(status().equals("started")) {
			ActiveMQServer activeBroker = broker.getActiveMQServer();
			String info = "uptime="+ activeBroker.getUptime() 
					 + ",totalConnections=" + activeBroker.getTotalConnectionCount()
					 + ",totalConsumers=" + activeBroker.getTotalConsumerCount()
					 + ",totalMessages=" + activeBroker.getTotalMessageCount()
					 + ",nodeId=" + activeBroker.getNodeID()
					 + ",state=" + activeBroker.getState()
					 + ",version=" + activeBroker.getVersion().getFullVersion()
					 + ",type=ActiveMQ Artemis";
			return info;
		}else {
			return "no info. broker not running";
		}
		
		
	}
	
	
	public String getFileConfiguration() throws IOException {

		if(!brokerFile.exists()) {
			ClassLoader classloader = Thread.currentThread().getContextClassLoader();
			
    		try {
    			FileUtils.touch(brokerFile);
    			InputStream is = classloader.getResourceAsStream("broker.xml");
    			Files.copy(is, brokerFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        		is.close();
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
    	
		return FileUtils.readFileToString(brokerFile, StandardCharsets.UTF_8);
					
	}
	
	public String setFileConfiguration(String brokerConfiguration) throws IOException {
		
		ClassLoader classloader = Thread.currentThread().getContextClassLoader();
		
		if(brokerFile.exists() || !brokerConfiguration.isEmpty()) {

			URL schemaFile = classloader.getResource("broker.xsd");
			String xmlValidation = ConnectorUtil.isValidXML(schemaFile, brokerConfiguration);
			if(!xmlValidation.equals("xml is valid")) {
				return xmlValidation;
			} 
			FileUtils.writeStringToFile(brokerFile, brokerConfiguration,StandardCharsets.UTF_8);
		}else {
			FileUtils.touch(brokerFile);
			InputStream is = classloader.getResourceAsStream("broker.xml");
			Files.copy(is, brokerFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
			is.close();
		}

		return "configuration set";
	}


	//Manage queues
	public String createQueue(String queueName) throws Exception {
		manageBroker.createQueue(queueName, "ANYCAST", queueName,"",true,-1,false,true );
		return "success";
	}

	public String deleteQueue(String queueName) throws Exception {
		manageBroker.deleteAddress(queueName,true);
		return "success";
	}

	public String getQueue(String endpointName) throws Exception {

		endpointExist = checkIfEndpointExist(endpointName);

		JSONObject endpointInfo = new JSONObject();

		if (endpointExist) {
			JSONObject endpoint = getEndpoint(endpointName);
			endpointInfo.put("queue",endpoint);
		}else{
			throw new Exception("Endpoint " + endpointName + " not found");
		}

		return endpointInfo.toString();
	}

	public String getQueues() throws Exception {

		JSONObject endpointsInfo  = new JSONObject();
		JSONObject endpointInfo = new JSONObject();

		String[] endpoints = manageBroker.getQueueNames("ANYCAST");

		for(String endpoint: endpoints){
			endpointInfo.append("queue", getEndpoint(endpoint));
		}

		endpointsInfo.put("queues",endpointInfo);

		return endpointsInfo.toString();
	}

	public String clearQueue(String queueName) throws Exception {

		ActiveMQServer activeBroker = broker.getActiveMQServer();
		Queue queue = activeBroker.locateQueue(new SimpleString(queueName));
		if (queue != null) {
			queue.deleteAllReferences();
		}

		return "success";
	}

	public String clearQueues() throws Exception {

		ActiveMQServer activeBroker = broker.getActiveMQServer();

		for (String queueName : manageBroker.getQueueNames("ANYCAST")) {
			Queue queue = activeBroker.locateQueue(new SimpleString(queueName));
			if (queue != null) {
				queue.deleteAllReferences();
			}
		}

		return "success";
	}

	//Manage topics
	public String deleteTopic(String topicName) throws Exception {
		manageBroker.deleteAddress(topicName,true);
		return "success";
	}

	public String createTopic(String topicName) throws Exception {
		manageBroker.createQueue(topicName, "MULTICAST", topicName,"",true,-1,false,true );
		return "success";
	}


	public String clearTopic(String topicName) throws Exception {

		ActiveMQServer activeBroker = broker.getActiveMQServer();
		Queue queue = activeBroker.locateQueue(new SimpleString(topicName));
		if (queue != null) {
			queue.deleteAllReferences();
		}

		return "success";
	}

	public String clearTopics() throws Exception {

		ActiveMQServer activeBroker = broker.getActiveMQServer();

		for (String queueName : manageBroker.getQueueNames("MULTICAST")) {
			Queue queue = activeBroker.locateQueue(new SimpleString(queueName));
			if (queue != null) {
				queue.deleteAllReferences();
			}
		}

		return "success";
	}

	public String getTopic(String endpointName) throws Exception {

		ActiveMQServer activeBroker = broker.getActiveMQServer();
		Queue queue = activeBroker.locateQueue(new SimpleString(endpointName));

		JSONObject endpointInfo = new JSONObject();

		if (queue == null) {
			endpointInfo.put("topic","Endpoint " + endpointName + " not found");
		}else{
			JSONObject endpoint = getEndpoint(endpointName);
			endpointInfo.put("topic",endpoint);
		}

		return endpointInfo.toString();	}

	public String getTopics() throws Exception {

		JSONObject endpointsInfo  = new JSONObject();
		JSONObject endpointInfo = new JSONObject();

		String[] endpoints = manageBroker.getQueueNames("MULTICAST");

		for(String endpoint: endpoints){
			endpointInfo.append("topic", getEndpoint(endpoint));
		}

		endpointsInfo.put("topics",endpointInfo);

		return endpointsInfo.toString();
	}

	private JSONObject getEndpoint(String endpointName) throws Exception {

		JSONObject endpoint = new JSONObject();

		ActiveMQServer activeBroker = broker.getActiveMQServer();

		QueueControl queueControl = (QueueControl) activeBroker.getManagementService().getResource(org.apache.activemq.artemis.api.core.management.ResourceNames.QUEUE + endpointName);

		endpoint.put("name",endpointName);
		endpoint.put("address",queueControl.getAddress());
		//endpoint.put("routingType",queueControl.getRoutingType());
		//endpoint.put("durable",queueControl.isDurable());
		//endpoint.put("exclusive",queueControl.isExclusive());
		endpoint.put("temporary",queueControl.isTemporary());
		endpoint.put("numberOfMessages",queueControl.countMessages());
		endpoint.put("numberOfConsumers",queueControl.getConsumerCount());

		return endpoint;

	}

	private boolean checkIfEndpointExist(String endpointName) throws Exception {

		ActiveMQServer activeBroker = broker.getActiveMQServer();
		Queue queue = activeBroker.locateQueue(new SimpleString(endpointName));

		if (queue == null) {
			return false;
		}else{
			return true;
		}
	}

	//Manage Messages
	public String moveMessage(String sourceQueueName, String targetQueueName, String messageId) throws Exception {

		endpointExist = checkIfEndpointExist(sourceQueueName);

		if (!endpointExist) {
			throw new Exception("Endpoint " + sourceQueueName + " not found");
		}

		ActiveMQServer activeBroker = broker.getActiveMQServer();

		QueueControl queueControl = (QueueControl) activeBroker.getManagementService().getResource(org.apache.activemq.artemis.api.core.management.ResourceNames.QUEUE + sourceQueueName);

		boolean result = queueControl.moveMessage(Long.parseLong(messageId),targetQueueName);

		return Boolean.toString(result);

	}

	public String moveMessages(String sourceQueueName, String targetQueueName) throws Exception {

		endpointExist = checkIfEndpointExist(sourceQueueName);

		if (!endpointExist) {
			throw new Exception("Endpoint " + sourceQueueName + " not found");
		}


		ActiveMQServer activeBroker = broker.getActiveMQServer();

		QueueControl queueControl = (QueueControl) activeBroker.getManagementService().getResource(org.apache.activemq.artemis.api.core.management.ResourceNames.QUEUE + sourceQueueName);

		int result = queueControl.moveMessages("", targetQueueName);

		return Integer.toString(result);

	}

	public String removeMessage(String endpointName, String messageId) throws Exception {

		endpointExist = checkIfEndpointExist(endpointName);

		if (!endpointExist) {
			throw new Exception("Endpoint " + endpointName + " not found");
		}


		ActiveMQServer activeBroker = broker.getActiveMQServer();

		QueueControl queueControl = (QueueControl) activeBroker.getManagementService().getResource(org.apache.activemq.artemis.api.core.management.ResourceNames.QUEUE + endpointName);

		boolean result = queueControl.removeMessage(Integer.parseInt(messageId));

		return Boolean.toString(result);

	}


	public String removeMessages(String endpointName) throws Exception {

		endpointExist = checkIfEndpointExist(endpointName);

		if (!endpointExist) {
			throw new Exception("Endpoint " + endpointName + " not found");
		}

		ActiveMQServer activeBroker = broker.getActiveMQServer();

		QueueControl queueControl = (QueueControl) activeBroker.getManagementService().getResource(org.apache.activemq.artemis.api.core.management.ResourceNames.QUEUE + endpointName);

		int result = queueControl.removeAllMessages();

		return Integer.toString(result);

	}

	public String listMessages(String endpointName, String filter) throws Exception {

		endpointExist = checkIfEndpointExist(endpointName);

		JSONObject messagesInfo = new JSONObject();
		JSONObject messageInfo = new JSONObject();

		if (!endpointExist) {
			throw new Exception("Endpoint " + endpointName + " not found");
		}

		if(filter==null){
			filter = "";
		}

		ActiveMQServer activeBroker = broker.getActiveMQServer();

		QueueControl queueControl = (QueueControl) activeBroker.getManagementService().getResource(org.apache.activemq.artemis.api.core.management.ResourceNames.QUEUE + endpointName);

		String messages = queueControl.listMessagesAsJSON(filter);

		JSONArray messagesArray = new JSONArray(messages);

		for (int i = 0 ; i < messagesArray.length(); i++) {
			JSONObject message = messagesArray.getJSONObject(i);
			messageInfo.append("message",message);
		}

		messagesInfo.put("messages",messageInfo);

		return messagesInfo.toString();

	}

	public String countMessages(String endpointName) throws Exception {

		endpointExist = checkIfEndpointExist(endpointName);

		if (!endpointExist) {
			throw new Exception("Endpoint " + endpointName + " not found");
		}

		ActiveMQServer activeBroker = broker.getActiveMQServer();

		QueueControl queueControl = (QueueControl) activeBroker.getManagementService().getResource(org.apache.activemq.artemis.api.core.management.ResourceNames.QUEUE + endpointName);

		long numberOfMessages = queueControl.getMessageCount();

		return Long.toString(numberOfMessages);

	}



	public String browseMessage(String endpointName, String messageId, boolean excludeBody) throws Exception {

		if (!endpointExist) {
			throw new Exception("Endpoint " + endpointName + " not found");
		}

		ActiveMQServer activeBroker = broker.getActiveMQServer();

		QueueControl queueControl = (QueueControl) activeBroker.getManagementService().getResource(org.apache.activemq.artemis.api.core.management.ResourceNames.QUEUE + endpointName);

		CompositeData[] messages = queueControl.browse();

		messages = stream(messages).filter(compositeData -> compositeData.get("messageID").equals(messageId)).toArray(CompositeData[]::new);

		String result = CompositeDataConverter.convertToJSON(messages, null,false, excludeBody);

		return result;

	}

	public String browseMessages(String endpointName, Integer page, Integer numberOfMessages, boolean excludeBody) throws Exception {

		endpointExist = checkIfEndpointExist(endpointName);

		if (!endpointExist) {
			throw new Exception("Endpoint " + endpointName + " not found");
		}

		ActiveMQServer activeBroker = broker.getActiveMQServer();

		QueueControl queueControl = (QueueControl) activeBroker.getManagementService().getResource(org.apache.activemq.artemis.api.core.management.ResourceNames.QUEUE + endpointName);

		CompositeData[] messages;

		if(page != null && numberOfMessages != null){
			messages = queueControl.browse(page,numberOfMessages);
		}else{

			Long countMessages = queueControl.countMessages();

			if(countMessages > 10000){
				throw new Exception("Maximum returned messages is 10000. Use paging when there are more than 10000 on the queue");
			}else{
				messages = queueControl.browse();
			}

		}

		String result = CompositeDataConverter.convertToJSON(messages, null,false, excludeBody);

		return result;

	}

	public String sendMessage(String endpointName, Map<String,Object> messageHeaders, String messageBody) throws Exception {

		endpointExist = checkIfEndpointExist(endpointName);

		if (!endpointExist) {
			throw new Exception("Endpoint " + endpointName + " not found");
		}

		String userName = broker.getActiveMQServer().getConfiguration().getClusterUser();
		String password = broker.getActiveMQServer().getConfiguration().getClusterPassword();

		Map<String,String> messageHeadersAsString = messageHeaders.entrySet().stream()
				.collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));


		ActiveMQServer activeBroker = broker.getActiveMQServer();

		QueueControl queueControl = (QueueControl) activeBroker.getManagementService().getResource(org.apache.activemq.artemis.api.core.management.ResourceNames.QUEUE + endpointName);

		String result = queueControl.sendMessage(messageHeadersAsString, Message.TEXT_TYPE, messageBody, true, userName, password);

		return result;

	}

	public String getConsumers() throws Exception {
		String consumersList = manageBroker.listAllConsumersAsJSON();

		JSONObject consumers = new JSONObject();
		consumers.put("consumers",new JSONArray(consumersList));

		return consumers.toString();
	}

	public String getConnections() throws Exception {
		String connectionsList = manageBroker.listConnectionsAsJSON();

		JSONObject connections = new JSONObject();
		connections.put("connections",new JSONArray(connectionsList));

		return connections.toString();
	}

	@Override
	public Object getBroker() throws Exception {
		return broker;
	}

	private void setManageBroker(){
		ActiveMQServer activeBroker = broker.getActiveMQServer();
		ActiveMQServerControlImpl activeBrokerControl = activeBroker.getActiveMQServerControl();

		manageBroker = activeBrokerControl;
	}

}
