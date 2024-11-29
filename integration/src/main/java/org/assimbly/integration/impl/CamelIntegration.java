package org.assimbly.integration.impl;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.JsonEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ScanResult;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.jms.pool.PooledConnectionFactory;
import org.apache.camel.*;
import org.apache.camel.api.management.ManagedCamelContext;
import org.apache.camel.api.management.mbean.ManagedCamelContextMBean;
import org.apache.camel.api.management.mbean.ManagedRouteMBean;
import org.apache.camel.builder.ThreadPoolProfileBuilder;
import org.apache.camel.catalog.DefaultCamelCatalog;
import org.apache.camel.catalog.EndpointValidationResult;
import org.apache.camel.component.direct.DirectComponent;
import org.apache.camel.component.jetty12.JettyHttpComponent12;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.component.kamelet.KameletComponent;
import org.apache.camel.component.metrics.messagehistory.MetricsMessageHistoryFactory;
import org.apache.camel.component.metrics.messagehistory.MetricsMessageHistoryService;
import org.apache.camel.component.metrics.routepolicy.MetricsRegistryService;
import org.apache.camel.component.metrics.routepolicy.MetricsRoutePolicyFactory;
import org.apache.camel.component.properties.PropertiesComponent;
import org.apache.camel.component.seda.SedaComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.language.xpath.XPathBuilder;
import org.apache.camel.model.ModelCamelContext;
import org.apache.camel.model.RouteConfigurationDefinition;
import org.apache.camel.spi.*;
import org.apache.camel.support.CamelContextHelper;
import org.apache.camel.support.DefaultExchange;
import org.apache.camel.support.PluginHelper;
import org.apache.camel.support.ResourceHelper;
import org.apache.camel.support.jsse.SSLContextParameters;
import org.apache.camel.util.concurrent.ThreadPoolRejectedPolicy;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.assimbly.dil.blocks.beans.*;
import org.assimbly.dil.blocks.beans.enrich.EnrichStrategy;
import org.assimbly.dil.blocks.beans.json.JsonAggregateStrategy;
import org.assimbly.dil.blocks.beans.xml.XmlAggregateStrategy;
import org.assimbly.dil.blocks.connections.Connection;
import org.assimbly.dil.blocks.processors.*;
import org.assimbly.dil.event.EventConfigurer;
import org.assimbly.dil.event.domain.Collection;
import org.assimbly.dil.loader.FlowLoader;
import org.assimbly.dil.loader.FlowLoaderReport;
import org.assimbly.dil.loader.RouteLoader;
import org.assimbly.dil.transpiler.XMLFileConfiguration;
import org.assimbly.dil.transpiler.ssl.SSLConfiguration;
import org.assimbly.dil.validation.*;
import org.assimbly.dil.validation.beans.FtpSettings;
import org.assimbly.dil.validation.beans.Regex;
import org.assimbly.dil.validation.beans.script.EvaluationRequest;
import org.assimbly.dil.validation.beans.script.EvaluationResponse;
import org.assimbly.docconverter.DocConverter;
import org.assimbly.util.*;
import org.assimbly.util.error.ValidationErrorMessage;
import org.assimbly.util.file.DirectoryWatcher;
import org.assimbly.util.helper.JsonHelper;
import org.assimbly.util.mail.ExtendedHeaderFilterStrategy;
import org.jasypt.properties.EncryptableProperties;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.yaml.snakeyaml.Yaml;

import javax.management.JMX;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.net.ssl.SSLContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStoreException;
import java.security.cert.Certificate;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class CamelIntegration extends BaseIntegration {

	private CamelContext context;
	private boolean started;
	private static String BROKER_HOST = "ASSIMBLY_BROKER_HOST";
	private static String BROKER_PORT = "ASSIMBLY_BROKER_PORT";
	private final static long stopTimeout = 500;
	private ServiceStatus status;
	private String flowStatus;
	private final MetricRegistry metricRegistry = new MetricRegistry();
	private org.apache.camel.support.SimpleRegistry registry = new org.apache.camel.support.SimpleRegistry();
	private String flowInfo;
	private RouteController routeController;
	private ManagedCamelContext managed;
	private Properties encryptionProperties;
	private boolean watchDeployDirectoryInitialized;
	private TreeMap<String, String> props;
	private TreeMap<String, String> confFiles = new TreeMap<String, String>();
	private String loadReport;
	private final Map<String, FlowLoaderReport> flowLoaderReportMap = new ConcurrentHashMap<>();

	private final String baseDir = BaseDirectory.getInstance().getBaseDirectory();
	private final String SEP = "/";
	private final String SECURITY_PATH = "security";
	private final String TRUSTSTORE_FILE = "truststore.jks";
	private final String KEYSTORE_FILE = "keystore.jks";
	private final String KEYSTORE_PWD = "KEYSTORE_PWD";

	private final String HTTP_MUTUAL_SSL_PROP = "httpMutualSSL";
	private final String RESOURCE_PROP = "resource";
	private final String AUTH_PASSWORD_PROP = "authPassword";

	public CamelIntegration() throws Exception {
		super();
		context = new DefaultCamelContext(registry);
	}

	public CamelIntegration(boolean useDefaultSettings) throws Exception {
		super();
		context = new DefaultCamelContext(registry);
		init(useDefaultSettings);
	}

	public final void init(boolean useDefaultSettings) throws Exception {

		//setting tracing standby to true, so it can be enabled during runtime
		context.setTracingStandby(true);

		//load settings into a separate thread
		if(useDefaultSettings){
			setDefaultSettings();
		}

		//set management tasks
		routeController = context.getRouteController();
		managed = context.getCamelContextExtension().getContextPlugin(ManagedCamelContext.class);

	}

	public void setDefaultSettings() throws Exception {

		setRouteTemplates();

		setGlobalOptions();

		setDefaultBlocks();

		setDefaultThreadProfile(5,50,5000);

		setThreadProfile("wiretapProfile", 0,10,2500);

		setCertificateStore(true);

		setDebugging(false);

		setSuppressLoggingOnTimeout(true);

		setStreamCaching(true);

		setMetrics(true);

		setHistoryMetrics(true);

		//setTracing(true,"backlog");

	}

	public void setTracing(boolean tracing, String traceType) {

		if(traceType.equalsIgnoreCase("backlog")){
			context.setBacklogTracing(true);
		}else if (traceType.equalsIgnoreCase("default")) {
			Tracer tracer = context.getTracer();
			tracer.setEnabled(tracing);
		}

	}

	public void setDebugging(boolean debugging) {
		context.setDebugging(debugging);
	}

	public void setStreamCaching(boolean streamCaching) {
		context.setStreamCaching(streamCaching);
		context.getStreamCachingStrategy().setBufferSize(32 * 1024);

		// --> When spooling is enabled it will cache the stream of large files to disk
		// --> The threshold is by default 128kb. Below is changed to 256KB
		// --> Turn the spooling off, because lead to issues ("Cannot reset stream")
		//context.getStreamCachingStrategy().setSpoolEnabled(true);
		//context.getStreamCachingStrategy().setSpoolDirectory(baseDir + "/streamcache");
		//context.getStreamCachingStrategy().setSpoolThreshold(256 * 1024);

	}

	public void setSuppressLoggingOnTimeout(boolean suppressLoggingOnTimeout) {
		context.getShutdownStrategy().setSuppressLoggingOnTimeout(suppressLoggingOnTimeout);
		context.getShutdownStrategy().setTimeUnit(TimeUnit.MILLISECONDS);
	}

	public void setCertificateStore(boolean certificateStore) throws Exception {
		if(certificateStore){
			setSSLContext();
		}
	}

	public void setMetrics(boolean metrics){
		if(metrics){
			context.addRoutePolicyFactory(new MetricsRoutePolicyFactory());
		}
	}

	public void setHistoryMetrics(boolean setHistoryMetrics){
		//set history metrics
		MetricsMessageHistoryFactory factory = new MetricsMessageHistoryFactory();
		factory.setPrettyPrint(true);
		factory.setMetricsRegistry(metricRegistry);
		context.setMessageHistoryFactory(factory);
		context.setSourceLocationEnabled(true);
	}

	public void setDefaultBlocks() throws Exception {

		//Add services
		context.addService(new org.assimbly.mail.component.mail.MailComponent());
		context.addService(new org.assimbly.mail.dataformat.mime.multipart.MimeMultipartDataFormat());
		context.addService(new org.assimbly.xmltojson.CustomXmlJsonDataFormat());

		DirectComponent directComponent = new DirectComponent();
		directComponent.setTimeout(300000);

		//Add components to a custom name
		context.addComponent("sync", directComponent);
		context.addComponent("async", new SedaComponent());

		KameletComponent kameletComponent = new KameletComponent();
		//kameletComponent.setLocation("ref:");
		context.addComponent("function", kameletComponent);

		JettyHttpComponent12 jettyHttpComponent12 = new org.apache.camel.component.jetty12.JettyHttpComponent12();
		jettyHttpComponent12.setRequestHeaderSize(80000);
		jettyHttpComponent12.setResponseHeaderSize(80000);
		context.addComponent("jetty-nossl", jettyHttpComponent12);
		context.addComponent("jetty", jettyHttpComponent12);

		// Add bean/processors and other custom classes to the registry
		registry.bind("AggregateStrategy", new AggregateStrategy());
		registry.bind("AttachmentAttacher",new org.assimbly.mail.component.mail.AttachmentAttacher());
		registry.bind("CurrentAggregateStrategy", new AggregateStrategy());
		registry.bind("CurrentEnrichStrategy", new EnrichStrategy());
		registry.bind("CustomHttpHeaderFilterStrategy",new CustomHttpHeaderFilterStrategy());
		registry.bind("customHttpBinding", new CustomHttpBinding());
		registry.bind("ExtendedHeaderFilterStrategy", new ExtendedHeaderFilterStrategy());
		registry.bind("flowCookieStore", new org.assimbly.cookies.CookieStore());
		registry.bind("InputStreamToStringProcessor", new InputStreamToStringProcessor());
		registry.bind("JsonAggregateStrategy", new JsonAggregateStrategy());
		registry.bind("ManageFlowProcessor", new ManageFlowProcessor());
		registry.bind("multipartProcessor",new org.assimbly.multipart.processor.MultipartProcessor());
		registry.bind("RoutingRulesProcessor", new RoutingRulesProcessor());
		registry.bind("SetOriginalMessageProcessor", new SetOriginalMessageProcessor());
		registry.bind("SetBodyProcessor", new SetBodyProcessor());
		registry.bind("SetHeadersProcessor", new SetHeadersProcessor());
		registry.bind("SetPatternProcessor", new SetPatternProcessor());
		registry.bind("Unzip", new UnzipProcessor());
		registry.bind("uuid-function", new UuidExtensionFunction());
		registry.bind("XmlAggregateStrategy", new XmlAggregateStrategy());
		registry.bind("FlowLogger", new FlowLogger());
		registry.bind("exceptionAsJson", new ExceptionAsJsonProcessor());

		//registry.bind("SetLogProcessor", new SetLogProcessor());
		//registry.bind("JsonExchangeFormatter", new JsonExchangeFormatter());
		//registry.bind("opentelemetry", new OpenTelemetryLogProcessor());

	}

	public void setDefaultThreadProfile(int poolSize, int maxPoolSize, int maxQueueSize) {
		context.getExecutorServiceManager().getDefaultThreadPoolProfile().setPoolSize(poolSize);
		context.getExecutorServiceManager().getDefaultThreadPoolProfile().setMaxPoolSize(maxPoolSize);
		context.getExecutorServiceManager().getDefaultThreadPoolProfile().setMaxQueueSize(maxQueueSize);
	}

	public void setThreadProfile(String name, int poolSize, int maxPoolSize, int maxQueueSize) {
		ThreadPoolProfileBuilder builder = new ThreadPoolProfileBuilder(name);
		builder.poolSize(poolSize).maxPoolSize(maxPoolSize).maxQueueSize(maxQueueSize).rejectedPolicy(ThreadPoolRejectedPolicy.CallerRuns).keepAliveTime(10L);
		context.getExecutorServiceManager().registerThreadPoolProfile(builder.build());
	}

	public void setGlobalOptions(){

		//enable virtual threads
		//System.setProperty("camel.threads.virtual.enabled","true");

		context.setUseBreadcrumb(true);

		//enable performance stats
		context.getManagementStrategy().getManagementAgent().setLoadStatisticsEnabled(true);

		// Enable Jackson JSON type converter for more types.
		context.getGlobalOptions().put("CamelJacksonEnableTypeConverter", "true");
		// Allow Jackson JSON to convert to pojo types also
		// (by default, Jackson only converts to String and other simple types)
		context.getGlobalOptions().put("CamelJacksonTypeConverterToPojo", "true");

	}

	//loads templates in the template package
	public void setRouteTemplates() throws Exception {

		//load kamelets into Camel Context

		RoutesLoader loader = PluginHelper.getRoutesLoader(context);

		List<String> resourceNames = getKamelets();

		for(String resourceName: resourceNames){

			if(resourceName.equals("kamelets/resolve-pojo-schema-action.kamelet.yaml")){
				continue;
			}

			if(resourceName.equals("kamelets/djl-image-to-text-action.kamelet.yaml")){
				continue;
			}

			URL url;
			if(resourceName.startsWith("file:")){
				url = new URL(resourceName);
			}else{
				url = Resources.getResource(resourceName);
			}

			String resourceAsString = Resources.toString(url, StandardCharsets.UTF_8);

			registry.bind(StringUtils.substringBetween(resourceName, "kamelets/",".kamelet.yaml"),resourceAsString);

			Resource resource = convertKameletToStep(resourceName, resourceAsString);

			try{
				loader.loadRoutes(resource);
			}catch (Exception e){
				log.warn("could not load: " + resourceName + ". Reason: " + e.getMessage());
			}

		}

	}

	private List<String> getKamelets() throws IOException {

		List<String> kamelets = new ArrayList<>();

		// Add resource paths from classpath (/kamelets under resources)
		List<String> classpathNames;
		try (ScanResult scanResult = new ClassGraph().acceptPaths("kamelets").scan()) {
			classpathNames = scanResult.getAllResources().getPaths();
		}

		if(classpathNames != null && !classpathNames.isEmpty()){
			kamelets.addAll(classpathNames);
		}

		// Add resource paths from filepath (Kamelets .assimbly/kamelets directory)
		List<String> filepathNames = new ArrayList<>();

		File kameletDir = new File(baseDir + "/kamelets");
		if(!kameletDir.exists()){
			FileUtils.forceMkdir(kameletDir);
		}else{

			Files.walk(Paths.get(baseDir + "/kamelets"))
					.filter(path -> Files.isRegularFile(path) && path.toString().endsWith("kamelet.yaml"))
					.forEach(path -> filepathNames.add("file:///" + path.toString().replace("\\","/")));

			if(filepathNames != null && !filepathNames.isEmpty()){
				kamelets.addAll(filepathNames);
			}

		}

		return kamelets;

	}

	private Resource convertKameletToStep(String resourceName, String resourceAsString){

		String properties = "    properties:\n" +
				"      in:\n" +
				"          title: Source endpoint\n" +
				"          description: The Camel uri of the source endpoint.\n" +
				"          type: string\n" +
				"          default: kamelet:source\n" +
				"      out:\n" +
				"          title: Sink endpoint\n" +
				"          description: The Camel uri of the sink endpoint.\n" +
				"          type: string\n" +
				"          default: kamelet:sink\n" +
				"      routeId:\n" +
				"          title: Route ID\n" +
				"          description: The Camel route ID.\n" +
				"          type: string\n" +
				"      routeConfigurationId:\n" +
				"          title: RouteConfiguration ID\n" +
				"          description: The Camel routeconfiguration ID.\n" +
				"          type: string\n" +
				"          default: 0";

		//replace values
		if(resourceName.contains("action") && !resourceAsString.contains("kamelet:sink") ){
			resourceAsString = resourceAsString + "      - to:\n" +
					"          uri: \"kamelet:sink\"";
		}

		/*
		resourceAsString = StringUtils.replaceOnce(resourceAsString,"      steps:\n" +
					"        -", "      steps:\n" +
					"        - step:\n" +
					"            id: \"{{routeId}}\"\n" +
					"        -");
		 */

		/*
		resourceAsString = StringUtils.replaceOnce(resourceAsString,"      steps:\n" +
				"      -","      steps:\n" +
				"      - step:\n" +
				"          id: \"{{routeId}}\"\n" +
				"      -");
		 */

		if(resourceAsString.contains("route:")){

			/*
			resourceAsString = StringUtils.replaceOnce(resourceAsString,"steps:","steps:\n" +
					"          - step:\n" +
					"              id: \"{{routeId}}\"");
			 */

			resourceAsString = StringUtils.replaceOnce(resourceAsString,"route:","route:\n" +
					"      routeConfigurationId: \"{{routeConfigurationId}}\"");

		}

		resourceAsString = StringUtils.replaceOnce(resourceAsString,"  template:\n" +
				"    from:","  template:\n" +
				"    route:\n" +
				"      routeConfigurationId: \"{{routeConfigurationId}}\"\n" +
				"    from:");

		resourceAsString = StringUtils.replace(resourceAsString,"\"kamelet:source\"", "\"{{in}}\"");
		resourceAsString = StringUtils.replace(resourceAsString,"\"kamelet:sink\"", "\"{{out}}\"");
		resourceAsString = StringUtils.replace(resourceAsString,"kamelet:source", "\"{{in}}\"");
		resourceAsString = StringUtils.replace(resourceAsString,"kamelet:sink", "\"{{out}}\"");
		resourceAsString = StringUtils.replace(resourceAsString,"    properties:", properties,1);
		resourceName= StringUtils.substringAfter(resourceName, "kamelets/");

		Resource resource = ResourceHelper.fromString(resourceName, resourceAsString);

		return resource;

	}

	public String getListOfStepTemplates() throws IOException {

		List<String> kamelets = getKamelets();

		for (int i = 0; i < kamelets.size(); i++) {
			String kamelet = kamelets.get(i);
			if (kamelet.endsWith(".kamelet.yaml")) {
				kamelets.set(i, kamelet.substring(9, kamelet.length() - 13));
			}
		}

		Collections.sort(kamelets);

		JSONArray jsonArray = new JSONArray(kamelets);

		return jsonArray.toString();

	}

	public String getStepTemplate(String mediaType, String stepName) throws Exception {

		String resourceName = "kamelets/" + stepName + ".kamelet.yaml";
		URL	url = Resources.getResource(resourceName);
		String resourceAsString = Resources.toString(url, StandardCharsets.UTF_8);

		if(mediaType.contains("xml")){
			resourceAsString = DocConverter.convertYamlToXml(resourceAsString);
		}else if(mediaType.contains("json")){
			resourceAsString = DocConverter.convertYamlToJson(resourceAsString);
		}

		return resourceAsString;

	}



	// (Un)install files

	/* not used yet
	public void setRoutesDirectory(boolean deployOnEvent) throws Exception{

		Path path = Paths.get(baseDir + "/routes");

		//Create the deploy directory if not exist
		Files.createDirectories(path);

		RouteWatcherReloadStrategy reload = new RouteWatcherReloadStrategy();
		reload.setFolder(path.toString());
		//reload.setPattern("*.xml");
		//reload.setRecursive(true);

		context.addService(reload);
		reload.start();

	}*/

	public String addCollectorsConfiguration(String mediaType, String configuration) throws Exception{

		String result = "unconfigured";

		if(mediaType.contains("xml")){
			configuration = DocConverter.convertXmlToJson(configuration);
		}else if(mediaType.contains("yaml")){
			configuration = DocConverter.convertYamlToJson(configuration);
		}

		ObjectMapper mapper = new ObjectMapper();
		List<Collection> collections = Arrays.asList(mapper.readValue(configuration, Collection[].class));

		for(Collection collection: collections){

			EventConfigurer eventConfigurer = new EventConfigurer(collection.getId(), context);

			result = eventConfigurer.add(collection);

			if(!result.equalsIgnoreCase("configured")){
				break;
			}
		}

		return result;

	}

	public String serialize(String json) throws IOException {
		Gson gson = new Gson();
		String g = gson.toJson(json);
		return StringEscapeUtils.escapeEcmaScript(g);
	}

	public String addCollectorConfiguration(String collectorId, String mediaType, String configuration) throws Exception{

		if(mediaType.contains("xml")){
			configuration = DocConverter.convertXmlToJson(configuration);
		}else if(mediaType.contains("yaml")){
			configuration = DocConverter.convertYamlToJson(configuration);
		}

		EventConfigurer eventConfigurer = new EventConfigurer(collectorId, context);

		String result = eventConfigurer.add(configuration);

		return result;

	}

	public String removeCollectorConfiguration(String collectorId) throws Exception{

		EventConfigurer eventConfigurer = new EventConfigurer(collectorId, context);

		String result = eventConfigurer.remove(collectorId);

		return result;
	}


	public void setDeployDirectory(boolean deployOnStart, boolean deployOnEvent) throws Exception {

		Path path = Paths.get(baseDir + "/deploy");

		//Create the deploy directory if not exist
		Files.createDirectories(path);

		if(deployOnStart && deployOnEvent){
			checkDeployDirectory(path);
			watchDeployDirectory(path);
		}else if (deployOnStart){
			//Check & Start files found in the deploy directory
			checkDeployDirectory(path);
		}else if(deployOnEvent){
			//Monitor files in the deploy directory after start
			watchDeployDirectory(path);
		}

	}

	private void checkDeployDirectory(Path path) throws Exception {
		Files.walk(path)
				.filter(fPath -> fPath.toString().endsWith(".xml") || fPath.toString().endsWith(".json")  || fPath.toString().endsWith(".yaml"))
				.forEach(fPath -> {
					try{
						fileInstall(fPath);
					} catch (Exception e) {
						log.error("Check deploy directory "+ path.toString() + " + failed",e);
					}
				});
	}

	private void watchDeployDirectory(Path path) throws Exception {

		if(watchDeployDirectoryInitialized){
			return;
		}

		log.info("Deploy folder | Init watching for changes: " + path);
		watchDeployDirectoryInitialized = true;

		DirectoryWatcher watcher = new DirectoryWatcher.Builder()
				.addDirectories(path)
				.setPreExistingAsCreated(true)
				.build(new DirectoryWatcher.Listener() {
					private long diff;
					private long timeCreated;
					private Path pathCreated;

					public void onEvent(DirectoryWatcher.Event event, Path path) {
						switch (event) {
							case ENTRY_CREATE:
								log.info("Deploy folder | File created: " + path);
								try {
									pathCreated = path;
									timeCreated = System.currentTimeMillis();
									Long lastModified = FileUtils.lastModifiedFileTime(path.toFile()).toMillis();

									diff = timeCreated - lastModified;

									log.info("time modified: " + diff);

									if(diff < 5000){
										fileInstall(path);
									}

								} catch (Exception e) {
									log.error("FileInstall for created " + path.toString() + " failed",e);
								}
								break;
							case ENTRY_MODIFY:
								Long timeModified = System.currentTimeMillis();
								String pathAsString = path.toString();
								String fileName = FilenameUtils.getBaseName(pathAsString);

								diff = timeModified - timeCreated;

								if(diff > 5000){
									log.info("Deploy folder | File modified: " + path);
									try {
										if (path.equals(pathCreated) && confFiles.get(fileName)!= null){
											fileReinstall(path);
										}else if (confFiles.get(fileName)!= null){
											fileReinstall(path);
										}else{
											fileInstall(path);
										}
									} catch (Exception e) {
										log.error("FileInstall for modified " + path.toString() + " failed",e);
									}
								}

								break;

							case ENTRY_DELETE:
								log.info("Deploy folder | File deleted: " + path);
								try {
									fileUninstall(path);
								} catch (Exception e) {
									log.error("FileUnInstall for deleted " + path.toString() + " failed",e);
								}
								break;
						}
					}
				});

		watcher.start();

	}


	public void fileInstall(Path path) throws Exception {

		String pathAsString = path.toString();
		String fileName = FilenameUtils.getBaseName(pathAsString);
		String mediaType = FilenameUtils.getExtension(pathAsString);
		String configuration = FileUtils.readFileToString(new File(pathAsString), "UTF-8");

		confFiles.put(fileName,configuration);

		if(mediaType.contains("json")){
			configuration = DocConverter.convertJsonToXml(configuration);
			mediaType = "xml";

		}else if(mediaType.contains("yaml")){
			if(configuration.contains("dil")){
				configuration = DocConverter.convertYamlToXml(configuration);
			}else{

				String xmlRoute = "";
				Yaml yaml = new Yaml();

				if (configuration.startsWith("- from:") || configuration.contains("kind: Integration") || configuration.contains("kind: Kamelet")) {
					if (configuration.startsWith("- from:")){
						configuration= StringUtils.replace(configuration,"\n","\n  ");
					}

					configuration = StringUtils.substringAfter(configuration, "from:");

					configuration = "- route:\n" +
							"    id: " + fileName + "-1\n" +
							"    from:" + configuration;
				}

				String[] routes = StringUtils.splitByWholeSeparator(configuration,"- route:\n");

				for(String route: routes){

					route = "- route:\n" + route;

					List<Map<String,Object>> yamlRoutes = yaml.load(route);

					int index = 0;
					for(Map<String,Object> yamlRoute: yamlRoutes){

						Map<String,String> routeMap = (Map<String, String>) yamlRoute.get("route");

						String id = routeMap.get("id");
						if(id == null || id.isEmpty()){
							id = fileName + "-" + index++;
						}

						//put yaml route into xml route
						xmlRoute = xmlRoute + "<route id=\"" + id + "\"><yamldsl><![CDATA[" + route + "]]></yamldsl></route>";

					}

				}

				configuration = "<routes id=\"" + fileName + "\" xmlns=\"http://camel.apache.org/schema/spring\">" + xmlRoute + "</routes>";

			}

			mediaType = "xml";
		}

		String flowId = setFlowId(fileName, configuration);

		if(flowId!=null){
			log.info("File install flowid=" + flowId + " | path=" + pathAsString);
			String loadReport = installFlow(flowId, stopTimeout, mediaType, configuration);

			if(loadReport.contains("\"event\": \"error\"")||loadReport.contains("\"event\": \"failed\"") || loadReport.contains("message\": \"Failed to load flow\"")){
				log.error(loadReport);
			}else{
				log.info(loadReport);
			}
		}else{
			log.error("File install for " + pathAsString + " failed. Invalid configuration file.");
		}

	}

	public void fileReinstall(Path path) throws Exception {

		String pathAsString = path.toString();
		String fileName = FilenameUtils.getBaseName(pathAsString);
		String mediaType = FilenameUtils.getExtension(pathAsString);
		String oldConfiguration = confFiles.get(fileName);

		if(mediaType.contains("json")){
			oldConfiguration = DocConverter.convertJsonToXml(oldConfiguration);
		}else if(mediaType.contains("yaml")){
			oldConfiguration = DocConverter.convertYamlToXml(oldConfiguration);
		}

		String flowId = setFlowId(fileName, oldConfiguration);

		stopFlow(flowId, stopTimeout);

		fileInstall(path);

	}


	public void fileUninstall(Path path) throws Exception {

		String pathAsString = path.toString();
		String fileName = FilenameUtils.getBaseName(pathAsString);
		String mediaType = FilenameUtils.getExtension(pathAsString);
		String configuration = confFiles.get(fileName);

		confFiles.remove(fileName);

		if(mediaType.contains("json")){
			configuration = DocConverter.convertJsonToXml(configuration);
			mediaType = "xml";
		}else if(mediaType.contains("yaml")){
			configuration = DocConverter.convertYamlToXml(configuration);
			mediaType = "xml";
		}

		String flowId = setFlowId(fileName, configuration);

		if(flowId!=null){
			log.info("File uninstall flowid=" + flowId + " | path=" + pathAsString);
			stopFlow(flowId, stopTimeout);
		}else{
			log.error("File uninstall for " + pathAsString + " failed. FlowId is null.");
		}

	}

	public String setFlowId(String filename, String configuration) throws Exception {

		String flowId = null;

		String configurationUTF8 = new String(configuration.getBytes(StandardCharsets.UTF_8),StandardCharsets.UTF_8);

		if(IntegrationUtil.isXML(configurationUTF8)) {

			Document doc = DocConverter.convertStringToDoc(configurationUTF8);
			XPath xPath = XPathFactory.newInstance().newXPath();

			String root = doc.getDocumentElement().getTagName();

			if (root.equals("dil") || root.equals("integrations") || root.equals("flows")) {
				flowId = xPath.evaluate("//flows/flow[id='" + filename + "']/id", doc);
				if (flowId == null || flowId.isEmpty()) {
					flowId = xPath.evaluate("//flows/flow[1]/id", doc);
				}
				if (flowId == null || flowId.isEmpty()) {
					flowId = xPath.evaluate("//flows/flow[1]/name", doc);
				}
			} else if (root.equals("flow")) {
				flowId = xPath.evaluate("//flow[id='" + filename + "']/id", doc);
				if (flowId == null || flowId.isEmpty()) {
					flowId = xPath.evaluate("//flow/id", doc);
				}
				if (flowId == null || flowId.isEmpty()) {
					flowId = xPath.evaluate("//flow/name", doc);
				}
			} else if (root.equals("camelContext")) {
				flowId = xPath.evaluate("/camelContext/@id", doc);
				if (flowId == null || flowId.isEmpty()) {
					log.warn("Configuration: CamelContext element doesn't have an id attribute");
				}
			} else if (root.equals("routes")) {
				flowId = xPath.evaluate("/routes/@id", doc);
				if (flowId == null || flowId.isEmpty()) {
					log.warn("Configuration: routes element doesn't have an id attribute");
				}
			} else if (root.equals("route")) {
				flowId = xPath.evaluate("/route/@id", doc);
				if (flowId == null || flowId.isEmpty()) {
					log.warn("Configuration: routes element doesn't have an id attribute");
				}
			} else {
				log.warn("Unknown configuration. Either a DIL file (starting with a <dil> element) or Camel file (starting with <routes> element) is expected");
			}
		}else{
			flowId = filename;
		}

		return flowId;

	}

	//Manage integration

	public void start() throws Exception {

		// start Camel context
		if(!started){

			context.start();
			started = true;

			log.info("Runtime started");

		}

	}

	public void stop() throws Exception {
		super.getFlowConfigurations().clear();
		if (context != null){
			for (Route route : context.getRoutes()) {
				//routeController.stopRoute(route.getId(), stopTimeout, java.util.concurrent.TimeUnit.Seconds);
				routeController.stopRoute(route.getId());
				context.removeRoute(route.getId());
			}

			context.stop();
			started = false;
			log.info("Integration stopped");

		}
	}

	public boolean isStarted() {
		return started;
	}



	//Manage flows

	public String addFlow(TreeMap<String, String> props)  {

		try{
			// add custom connections if needed
			addCustomActiveMQConnection(props, "dovetail");

			//create connections & install dependencies if needed
			createConnections(props);

			HashMap<String, String> mutualSSLInfoMap = getMutualSSLInfoFromProps(props);
			if(mutualSSLInfoMap!=null && !mutualSSLInfoMap.isEmpty()) {
				// add certificate on keystore
				addCertificateFromUrl(mutualSSLInfoMap.get(RESOURCE_PROP), mutualSSLInfoMap.get(AUTH_PASSWORD_PROP));
			}

			FlowLoader flow = new FlowLoader(props, getFlowLoaderReport(props.get("id")));

			flow.addRoutesToCamelContext(context);

			loadReport = flow.getReport();

			if(!flow.isFlowLoaded()){
				return "error";
			}

			return "started";

		}catch (Exception e){
			log.error("add flow failed: ", e);
			return "error reason: " + e.getMessage();
		}

	}

	// add certificate from url on the keystore
	private void addCertificateFromUrl(String url, String authPassword) {
		try {
			byte[] fileContent;
			try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(IOUtils.toByteArray(new URL(url)))) {
				fileContent = IOUtils.toByteArray(byteArrayInputStream);
			}
			String encodedResourceContent = Base64.getEncoder().encodeToString(fileContent);

			CertificatesUtil util = new CertificatesUtil();
			String keystorePath = baseDir + SEP + SECURITY_PATH + SEP + KEYSTORE_FILE;
			util.importP12Certificate(keystorePath, getKeystorePassword(), encodedResourceContent, authPassword);

		} catch (Exception e) {
			log.error("Error to add certificate", e);
		}
	}

	// get mutual ssl info from props
	private HashMap<String, String> getMutualSSLInfoFromProps(TreeMap<String, String> props) {
		for (Map.Entry<String, String> entry : props.entrySet()) {
			String xml = entry.getValue();
			if(!xml.startsWith("<")) {
				// skip prop if it's not an xml
				continue;
			}
			HashMap<String, String> mutualSSLMap = getMutualSSLInfoFromXml(xml);
			if(mutualSSLMap!=null && !mutualSSLMap.isEmpty()) {
				return mutualSSLMap;
			}
		}
		return null;
	}

	// get mutual ssl info from xml
	private HashMap<String, String> getMutualSSLInfoFromXml(String xml) {
		HashMap<String, String> map = new HashMap<>();

		String httpMutualSSL = getPropertyValue(xml, HTTP_MUTUAL_SSL_PROP);
		if(httpMutualSSL!=null && httpMutualSSL.equals("true")) {
			String authPassword = getPropertyValue(xml, AUTH_PASSWORD_PROP);
			String resource = getPropertyValue(xml, RESOURCE_PROP);
			if(authPassword!=null && resource!=null) {
				map.put(AUTH_PASSWORD_PROP, authPassword);
				map.put(RESOURCE_PROP, resource);
			}
		}

		return map;
	}

	// get property value by property name
	private String getPropertyValue(String xml, String propName) {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		XPathFactory xpathFactory = XPathFactory.newInstance();
		XPath xpath = xpathFactory.newXPath();
		try {
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document doc = builder.parse(new InputSource(new java.io.StringReader(xml)));
			String expression = String.format("//setProperty[@name='%s']/constant/text()", propName);
			return xpath.evaluate(expression, doc);
		} catch (Exception e) {
			// do nothing
		}

		return null;
	}

	private void addCustomActiveMQConnection(TreeMap<String, String> props, String frontendEngine) {

		try {

			String activemqName = "activemq";
			String brokerHost = System.getenv(BROKER_HOST);
			String brokerPort = System.getenv(BROKER_PORT);
			String activemqUrl = (
					brokerHost!=null && brokerPort!=null ?
							String.format("tcp://%s:%s", brokerHost, brokerPort) :
							"tcp://localhost:61616"
			);

			Component activemqComp = this.context.getComponent(activemqName);

			if (activemqComp == null) {
				JmsComponent jmsComponent = getJmsComponent(activemqUrl);
				this.context.addComponent(activemqName, jmsComponent);
			}

		} catch (Exception e) {
			log.error("Error to add custom activeMQ connection", e);
		}

	}

	private static JmsComponent getJmsComponent(String activemqUrl) {

		int maxConnections = getEnvironmentVarAsInteger("AMQ_MAXIMUM_CONNECTIONS",500);
		int idleTimeout = getEnvironmentVarAsInteger("AMQ_IDLE_TIMEOUT",5000);

		ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(activemqUrl);

		PooledConnectionFactory pooledConnectionFactory = new PooledConnectionFactory();
		pooledConnectionFactory.setConnectionFactory(activeMQConnectionFactory);
		pooledConnectionFactory.setMaxConnections(maxConnections); // Max connections in the pool
		pooledConnectionFactory.setIdleTimeout(idleTimeout);  // Idle timeout in milliseconds

		JmsComponent jmsComponent = new JmsComponent();
		jmsComponent.setConnectionFactory(pooledConnectionFactory);

		return jmsComponent;
	}

	public void createConnections(TreeMap<String, String> props) throws Exception {

		for (String key : props.keySet()){

			if (key.endsWith("connection.id")){
				setConnection(props, key);
			}

			if (key.equals("flow.dependencies") && props.get(key) != null){

				String[] schemes = StringUtils.split(props.get(key), ",");

				for (String scheme : schemes) {
					if(context.getComponent(scheme.toLowerCase()) == null) {
						if(!DependencyUtil.CompiledDependency.hasCompiledDependency(scheme.toLowerCase())) {
							log.warn("Component " + scheme + " is not supported by Assimbly. Try to resolve dependency dynamically.");
							if(INetUtil.isHostAvailable("repo1.maven.org")){
								log.info(resolveDependency(scheme));
							}else{
								log.error("Failed to resolve " + scheme + ". No available internet is found. Cannot reach http://repo1.maven.org/maven2/");
							}
						}
					}
				}
			}
		}
	}

	public void addEventNotifier(EventNotifier eventNotifier) throws Exception {
		context.getManagementStrategy().addEventNotifier(eventNotifier);
		context.getManagementStrategy().getEventFactory().setTimestampEnabled(true);
	}

	public boolean removeFlow(String id) throws Exception {

		boolean removed = false;
		List<Route> routes = getRoutesByFlowId(id);

		if(routes!=null && routes.size() > 0){
			for(Route route: routes){
				route.getId();
				context.removeRoute(id);
				removed = true;
			}
		}

		return removed;

	}

	public boolean hasFlow(String id) {

		boolean routeFound = false;

		if (context != null){
			for (Route route : context.getRoutes()) {
				if (route.getId().startsWith(id)) {
					routeFound = true;
				}
			}
		}
		return routeFound;
	}

	public String startAllFlows() throws Exception {
		log.info("Starting all flows");

		List<TreeMap<String, String>> allProps = super.getFlowConfigurations();
		Iterator<TreeMap<String, String>> it = allProps.iterator();
		while(it.hasNext()){
			TreeMap<String, String> props = it.next();
			flowStatus = startFlow(props.get("id"), stopTimeout);
			if(!flowStatus.equals("started")) {
				return "failed to start flow with id " + props.get("id") + ". Status is " + flowStatus;
			}
		}

		return "started";
	}

	public String restartAllFlows() throws Exception {
		log.info("Restarting all flows");

		List<TreeMap<String, String>> allProps = super.getFlowConfigurations();
		Iterator<TreeMap<String, String>> it = allProps.iterator();
		while(it.hasNext()){
			TreeMap<String, String> props = it.next();
			flowStatus = restartFlow(props.get("id"), stopTimeout);
			if(!flowStatus.equals("restarted")) {
				return "failed to restart flow with id " + props.get("id") + ". Status is " + flowStatus;
			}
		}

		return "restarted";
	}

	public String pauseAllFlows() throws Exception {
		log.info("Pause all flows");
		List<TreeMap<String, String>> allProps = super.getFlowConfigurations();

		Iterator<TreeMap<String, String>> it = allProps.iterator();
		while(it.hasNext()){
			TreeMap<String, String> props = it.next();
			flowStatus = restartFlow(props.get("id"), stopTimeout);
			if(!flowStatus.equals("restarted")) {
				return "failed to restart flow with id " + props.get("id") + ". Status is " + flowStatus;
			}
		}

		return "paused";
	}

	public String resumeAllFlows() throws Exception {
		log.info("Resume all flows");

		List<TreeMap<String, String>> allProps = super.getFlowConfigurations();
		Iterator<TreeMap<String, String>> it = allProps.iterator();
		while(it.hasNext()){
			TreeMap<String, String> props = it.next();
			flowStatus = resumeFlow(props.get("id"));
			if(!flowStatus.equals("restarted")) {
				return "failed to resume flow with id " + props.get("id") + ". Status is " + flowStatus;
			}
		}

		return "started";
	}

	public String stopAllFlows() throws Exception {
		log.info("Stopping all flows");

		List<TreeMap<String, String>> allProps = super.getFlowConfigurations();
		Iterator<TreeMap<String, String>> it = allProps.iterator();
		while(it.hasNext()){
			TreeMap<String, String> props = it.next();
			flowStatus = stopFlow(props.get("id"), stopTimeout);
			if(!flowStatus.equals("restarted")) {
				return "failed to stop flow with id " + props.get("id") + ". Status is " + flowStatus;
			}
		}

		return "stopped";
	}

	public String configureAndRestartFlow(String flowId, long timeout, String mediaType, String configuration) throws Exception {
		super.setFlowConfiguration(flowId, mediaType, configuration);
		return restartFlow(flowId, timeout);
	}

	public String installFlow(String flowId, long timeout, String mediaType, String configuration) throws Exception {
		super.setFlowConfiguration(flowId, mediaType, configuration);
		return startFlow(flowId, timeout);
	}

	public String uninstallFlow(String flowId, long timeout) throws Exception {
		return stopFlow(flowId, timeout);
	}

	public String fileInstallFlow(String flowId, String configuration) throws Exception {

		try {
			File flowFile = new File(baseDir + "/deploy/" + flowId + ".xml");
			FileUtils.writeStringToFile(flowFile, configuration, Charset.defaultCharset());
			return "saved";
		} catch (Exception e) {
			log.error("FileInstall flow " + flowId + " failed",e);
			return "Fail to save flow " + flowId + " Error: " + e.getMessage();
		}

	}

	public String fileUninstallFlow(String flowId) throws Exception {

		try {
			File flowFile = new File(baseDir + "/deploy/" + flowId + ".xml");
			FileUtils.deleteQuietly(flowFile);
			return "deleted";
		} catch (Exception e) {
			log.error("FileUninstall flow " + flowId + " failed",e);
			return "failed to delete flow " + e.getMessage();
		}

	}

	public String installRoute(String routeId, String route) throws Exception {

		initFlowActionReport(routeId, "Start");

		if(!route.startsWith("<route")){
			route = new XMLFileConfiguration().getRouteConfiguration(routeId,route);
		}

		try{

			RouteLoader routeLoader = new RouteLoader(routeId,route,getFlowLoaderReport(routeId));

			routeLoader.addRoutesToCamelContext(context);

			loadReport = routeLoader.getReport();

			finishFlowActionReport(routeId, "start","Started flow successfully","info");
		}catch(Exception e){
			finishFlowActionReport(routeId, "error","Failed starting flow",e.getMessage());
		}

		return loadReport;

	}

	public String routesFlow(String flowId, String mediaType, String configuration) throws Exception {

		TreeMap<String, String> props = new TreeMap<>();
		props.put("id",flowId);
		props.put("flow.name",flowId);
		props.put("flow.type","esb");
		props.put("route.1.route", configuration);

		addFlow(props);

		String status = startFlow(flowId, stopTimeout);

		return status;

	}

	public String startFlow(String id, long timeout) {
		String uuid = UUID.randomUUID().toString();
		System.out.println(" >> startFlow > id: "+id+", uuid: "+uuid);

		initFlowActionReport(id, "Start");

		if(hasFlow(id)) {
			System.out.println("  > stopFlow > id: "+id+", uuid: "+uuid);
			stopFlow(id, timeout, false);
		}

		boolean addFlow = false;
		String result = "unloaded";

		try {
			System.out.println("    inside try > id: "+id+", uuid: "+uuid);
			List<TreeMap<String, String>> allProps = super.getFlowConfigurations();

			for (int i = 0; i < allProps.size(); i++) {
				props = allProps.get(i);

				String configureId = props.get("id");

				if (configureId.equals(id)) {
					addFlow = true;
				}

			}

			if(addFlow){
				System.out.println("    1.1 > id: "+id+", uuid: "+uuid);
				result = addFlow(props);
			}else{
				System.out.println("    1.2 > id: "+id+", uuid: "+uuid);
				String errorMessage = "Starting flow failed | Flow ID: " + id + " does not match Flow ID in configuration";
				finishFlowActionReport(id, "error",errorMessage,"error");
			}

			if (!result.equals("loaded") && !result.equals("started")){
				System.out.println("    2.1 > id: "+id+", uuid: "+uuid);
				if(result.equalsIgnoreCase("error")){
					String startReport = loadReport;
					stopFlow(id, timeout, false);
					loadReport = startReport;
				}else{
					finishFlowActionReport(id, "error",result,"error");
				}
			}else if(result.equals("loaded")) {
				System.out.println("    2.2 > id: "+id+", uuid: "+uuid);
				List<Route> steps = getRoutesByFlowId(id);

				log.info("Starting " + steps.size() + " steps");

				for (Route step : steps) {
					status = startStep(step);
				}

				if (status!= null && status.isStarted()) {
					finishFlowActionReport(id, "start","Started flow successfully","info");
				}else{
					finishFlowActionReport(id, "error","Failed starting flow","error");
				}

			}else if(result.equals("started")) {
				System.out.println("    2.3 > id: "+id+", uuid: "+uuid);
				finishFlowActionReport(id, "start","Started flow successfully","info");
			}

		}catch (Exception e) {
			if(context.isStarted()) {
				stopFlow(id, stopTimeout, false);
				finishFlowActionReport(id, "error","Start flow failed | error=" + e.getMessage(),"error");
				log.error("Start flow failed. | flowid=" + id,e);
			}else{
				finishFlowActionReport(id, "error","Start flow failed | error=Integration isn't running","error");
				log.error("Start flow failed. | flowid=" + id,e);
			}
		}

		return loadReport;

	}

	private ServiceStatus startStep(Route route){

		String routeId = route.getId();

		status = routeController.getRouteStatus(routeId);

		if(status.isStarted()) {
			log.info("Started step | stepid=" + routeId);
		} else {
			try {

				log.info("Starting step | stepid=" + routeId);

				routeController.startRoute(routeId);

				int count = 1;

				do {
					if(status.isStarted()) {break;}
					Thread.sleep(10);
					count++;

				} while (status.isStarting() || count < 3000);

			} catch (Exception e) {
				log.error("Failed starting step | stepid=" + routeId);
				return status;
			}

			log.info("Started step | stepid=" + routeId);

		}

		return status;
	}


	public String restartFlow(String id, long timeout) {

		try {
			startFlow(id, timeout);
		}catch (Exception e) {
			log.error("Restart flow failed. | flowid=" + id,e);
			finishFlowActionReport(id, "error", e.getMessage(),"error");
		}

		return loadReport;

	}

	public String stopFlow(String id, long timeout) {
		return stopFlow(id, timeout, true);
	}

	public String stopFlow(String id, long timeout, boolean enableReport) {

		if(enableReport) {
			initFlowActionReport(id, "stop");
		}

		try {

			List<Route> routeList = getRoutesByFlowId(id);

			for (Route route : routeList) {

				String routeId = route.getId();

				log.info("Stopping step id: " + routeId);

				//moved removal of routeConfiguration to the flowLoader
				//if(route.getConfigurationId()!=null) {
				//	removeRouteConfiguration(route.getConfigurationId());
				//}

				context.getRouteController().stopRoute(routeId,timeout, TimeUnit.MILLISECONDS);
				context.removeRoute(routeId);

			}

			if(enableReport) {
				finishFlowActionReport(id, "stop", "Stopped flow successfully", "info");
			}

		}catch (Exception e) {
			if(enableReport) {
				finishFlowActionReport(id, "error", "Stop flow failed | error=" + e.getMessage(), "error");
			}
			log.error("Stop flow failed. | flowid=" + id,e);
		}

		return loadReport;

	}

	private void removeRouteConfiguration(String routeConfigurationId) throws Exception {
		ModelCamelContext modelContext = (ModelCamelContext) context;
		RouteConfigurationDefinition routeConfigurationDefinition = modelContext.getRouteConfigurationDefinition(routeConfigurationId);
		if(routeConfigurationDefinition!=null){
			modelContext.removeRouteConfiguration(routeConfigurationDefinition);
			log.info("Removed routeConfiguration: " + routeConfigurationDefinition.getId());
		}
	}

	public String pauseFlow(String id) {

		initFlowActionReport(id, "pause");

		try {

			if(hasFlow(id)) {

				List<Route> routeList = getRoutesByFlowId(id);
				status = routeController.getRouteStatus(routeList.get(0).getId());

				for(Route route : routeList){
					if(!routeController.getRouteStatus(route.getId()).isSuspendable()){
						finishFlowActionReport(id, "error","Flow isn't suspendable (Step " + route.getId() + ")","error");
						return loadReport;
					}
				}

				for(Route route : routeList){


					String routeId = route.getId();

					routeController.suspendRoute(routeId);

					int count = 1;

					do {
						status = routeController.getRouteStatus(routeId);
						if(status.isSuspended()) {
							log.info("Paused (suspend) flow | flowid=" + id + "| stepid=" + routeId);
							break;
						}else if(status.isStopped()){
							log.info("Paused (stopped) flow | flowid=" + id + "| stepid=" + routeId);
							break;
						}

						Thread.sleep(10);
						count++;

					} while (status.isSuspending() || count < 6000);
				}
				finishFlowActionReport(id, "pause","Paused flow successfully","info");
			}else {
				String errorMessage = "Configuration is not set (use setConfiguration or setFlowConfiguration)";
				finishFlowActionReport(id, "error",errorMessage,"error");
			}
		}catch (Exception e) {
			log.error("Pause flow failed. | flowid=" + id,e);
			stopFlow(id, stopTimeout); //Stop flow if one of the routes cannot be paused.
			finishFlowActionReport(id, "error",e.getMessage(),"error");
		}

		return loadReport;

	}

	public String resumeFlow(String id) throws Exception {

		initFlowActionReport(id, "resume");

		try {

			if(hasFlow(id)) {
				boolean resumed = true;
				List<Route> routeList = getRoutesByFlowId(id);
				for(Route route : routeList){
					String routeId = route.getId();
					status = routeController.getRouteStatus(routeId);
					if(status.isSuspended()){
						routeController.resumeRoute(routeId);

						int count = 1;

						do {
							status = routeController.getRouteStatus(routeId);
							if(status.isStarted()) {break;}
							Thread.sleep(10);
							count++;

						} while (status.isStarting() || count < 3000);

						resumed = true;
						log.info("Resumed flow  | flowid=" + id + " | stepid=" + routeId);

					}
					else if (status.isStopped()){

						log.info("Starting route as route " + id + " is currently stopped (not suspended)");
						startFlow(routeId, stopTimeout);
						resumed = true;
					}
				}
				if(resumed){
					finishFlowActionReport(id, "resume","Resumed flow successfully","info");
				}else {
					finishFlowActionReport(id, "error","Flow isn't suspended (nothing to resume)","error");
				}
			}else {
				String errorMessage = "Configuration is not set (use setConfiguration or setFlowConfiguration)";
				finishFlowActionReport(id, "error",errorMessage,"error");
			}

		}catch (Exception e) {
			log.error("Resume flow " + id + " failed.",e);
			finishFlowActionReport(id, "error",e.getMessage(),"error");
		}

		return loadReport;

	}

	private void initFlowActionReport(String id, String event) {
		FlowLoaderReport flowLoaderReport = getFlowLoaderReport(id);
		flowLoaderReport.initReport(id, id, event);
	}

	private void finishFlowActionReport(String id, String event, String message, String messageType) {

		String eventCapitalized = StringUtils.capitalize(event);

		//logs event to
		if(messageType.equalsIgnoreCase("error")){
			log.error(eventCapitalized + " flow " + id + " failed | flowid=" + id,message);
		}else if(messageType.equalsIgnoreCase("warning"))
			log.warn(eventCapitalized + " flow" + id + " failed | flowid=" + id,message);
		else{
			log.info(message + " | flowid=" + id);
		}

		TreeMap<String, String> flowProps = null;
		try {
			flowProps = getFlowConfiguration(id);
			String version = flowProps.get("flow.version");
			String environment = flowProps.get("environment");

			if(version==null){
				version = "0";
			}

			if(environment==null){
				environment =  "";
			}

			getFlowLoaderReport(id).finishReport(id,id,event,version,environment,message);

		} catch (Exception e) {
			getFlowLoaderReport(id).finishReport(id,id,event,"","",message);
		}
		loadReport = getFlowLoaderReport(id).getReport();
		clearFlowLoaderReport(id);
	}

	public boolean isFlowStarted(String id) {

		if(hasFlow(id)) {
			ServiceStatus status = null;
			List<Route> routes = getRoutesByFlowId(id);

			for(Route route : routes){
				status = routeController.getRouteStatus(route.getId());

				if(!status.isStarted()){
					return false;
				}
			}
			return status != null && status.isStarted();
		}else {
			return false;
		}

	}

	public String getFlowInfo(String id, String mediaType) throws Exception {

		JSONObject json = new JSONObject();
		JSONObject flow = new JSONObject();

		TreeMap<String, String> props = super.getFlowConfiguration(id);

		if(props != null){
			flow.put("id",props.get("id"));
			flow.put("name",props.get("flow.name"));
			flow.put("version",props.get("flow.version"));
			flow.put("environment",props.get("environment"));
			flow.put("isRunning",isFlowStarted(id));
			flow.put("status",getFlowStatus(id));
			flow.put("uptime",getFlowUptime(id));
		}else{
			flow.put("id",id);
			flow.put("status",getFlowStatus(id));
		}

		json.put("flow",flow);

		String integrationInfo = json.toString(4);
		if(mediaType.contains("xml")) {
			integrationInfo = DocConverter.convertJsonToXml(integrationInfo);
		}

		return integrationInfo;

	}


	public String getFlowStatus(String id) {

		if(hasFlow(id)) {
			String updatedId;
			if(id.contains("-")){
				updatedId = id;
			}else{
				updatedId = id + "-";
			}

			try {
				List<Route> routesList = getRoutesByFlowId(updatedId);
				if(routesList.isEmpty()){
					log.info(" >> routesList is empty for id {} / updatedId {} > set state as unconfigured");
					flowStatus = "unconfigured";
				}else{
					String flowId = routesList.get(0).getId();
					ServiceStatus status = routeController.getRouteStatus(flowId);
					flowStatus = status.toString().toLowerCase();
				}
			}catch (Exception e) {
				log.error("Get status flow " + id + " failed.",e);

				flowStatus = "error: " + e.getMessage();
			}

		}else {
			log.info(" >> id {} not found on context routes > set state as unconfigured", id);
			flowStatus = "unconfigured";
		}

		return flowStatus;

	}

	public String getFlowUptime(String id) {

		String flowUptime;
		if(hasFlow(id)) {
			Route route = getRoutesByFlowId(id).get(0);
			flowUptime = route.getUptime();
		}else {
			flowUptime = "0";
		}

		return flowUptime;
	}

	public String getFlowLastError(String id) {

		List<Route> routeList = getRoutesByFlowId(id);
		StringBuilder sb = new StringBuilder();
		for(Route r : routeList){
			String routeId = r.getId();
			ManagedRouteMBean route = managed.getManagedRoute(routeId);

			if(route != null){
				org.apache.camel.api.management.mbean.RouteError lastError = route.getLastError();
				if(lastError != null){
					sb.append("RouteID: ");
					sb.append(routeId);
					sb.append("Error: ");
					sb.append(lastError.toString());
					sb.append(";");
				}
			}
		}
		if(sb.toString().isEmpty()){
			flowInfo = "0";
		} else{
			flowInfo = sb.toString();
		}

		return flowInfo;
	}

	public String getFlowMessages(String flowId, boolean includeSteps, String mediaType) throws Exception  {

		JSONObject json = new JSONObject();
		JSONObject flow = new JSONObject();
		JSONArray steps = new JSONArray();

		long totalMessages = 0;
		long completedMessages = 0;
		long failedMessages = 0;
		long pendingMessages = 0;

		List<Route> routes = getRoutesByFlowId(flowId);

		for(Route r : routes){
			String routeId = r.getId();

			ManagedRouteMBean route = managed.getManagedRoute(routeId);
			if(route != null){
				totalMessages += route.getExchangesTotal();
				completedMessages += route.getExchangesCompleted();
				failedMessages += route.getExchangesFailed();
				pendingMessages += route.getExchangesInflight();
				if(includeSteps){
					JSONObject step = new JSONObject();
					String stepId = StringUtils.substringAfter(routeId,flowId + "-");
					step.put("id", stepId);
					step.put("total",route.getExchangesTotal());
					step.put("completed",route.getExchangesCompleted());
					step.put("failed",route.getExchangesFailed());
					step.put("pending",route.getExchangesInflight());
					steps.put(step);
				}
			}
		}

		flow.put("id",flowId);
		flow.put("total",totalMessages);
		flow.put("completed",completedMessages);
		flow.put("failed",failedMessages);
		flow.put("pending",pendingMessages);
		if(includeSteps){
			flow.put("steps",steps);
		}
		json.put("flow",flow);

		String flowStats = json.toString(4);
		if(mediaType.contains("xml")) {
			flowStats = DocConverter.convertJsonToXml(flowStats);
		}

		return flowStats;

	}

	public String getFlowTotalMessages(String flowId) throws Exception {

		List<Route> routeList = getRoutesByFlowId(flowId);

		long totalMessages = 0;

		for(Route r : routeList){
			String routeId = r.getId();
			ManagedRouteMBean route = managed.getManagedRoute(routeId);

			if(route != null){
				totalMessages += route.getExchangesTotal();
			}
		}

		flowInfo = Long.toString(totalMessages);

		return flowInfo;

	}

	public String getFlowCompletedMessages(String flowId) throws Exception {

		List<Route> routeList = getRoutesByFlowId(flowId);
		long completedMessages = 0;

		for(Route r : routeList){
			String routeId = r.getId();

			ManagedRouteMBean route = managed.getManagedRoute(routeId);
			if(route != null){
				completedMessages += route.getExchangesCompleted();
			}
		}

		flowInfo = Long.toString(completedMessages);

		return flowInfo;

	}

	public String getFlowFailedMessages(String flowId) throws Exception  {

		List<Route> routeList = getRoutesByFlowId(flowId);
		long failedMessages = 0;

		for(Route r : routeList){
			String routeId = r.getId();

			ManagedRouteMBean route = managed.getManagedRoute(routeId);
			if(route != null){
				failedMessages += route.getExchangesFailed();
			}
		}

		flowInfo = Long.toString(failedMessages);

		return flowInfo;

	}

	public String getFlowPendingMessages(String flowId) throws Exception  {

		List<Route> routeList = getRoutesByFlowId(flowId);
		long pendingMessages = 0;

		for(Route r : routeList){
			String routeId = r.getId();

			ManagedRouteMBean route = managed.getManagedRoute(routeId);
			if(route != null){
				pendingMessages += route.getExchangesInflight();
			}
		}

		flowInfo = Long.toString(pendingMessages);

		return flowInfo;

	}

	public String getStepMessages(String flowId, String stepId, String mediaType) throws Exception  {

		long totalMessages = 0;
		long completedMessages = 0;
		long failedMessages = 0;
		long pendingMessages = 0;

		String routeId = flowId + "-" + stepId;

		ManagedRouteMBean route = managed.getManagedRoute(routeId);
		if(route != null){
			totalMessages += route.getExchangesTotal();
			completedMessages += route.getExchangesCompleted();
			failedMessages += route.getExchangesFailed();
			pendingMessages += route.getExchangesInflight();
		}

		JSONObject json = new JSONObject();
		JSONObject step = new JSONObject();

		step.put("id",flowId);
		step.put("total",totalMessages);
		step.put("completed",completedMessages);
		step.put("failed",failedMessages);
		step.put("pending",pendingMessages);
		json.put("step",step);

		String flowStats = json.toString(4);
		if(mediaType.contains("xml")) {
			flowStats = DocConverter.convertJsonToXml(flowStats);
		}

		return flowStats;

	}

	public String getFlowAlertsLog(String id, Integer numberOfEntries) throws Exception  {

		Date date = new Date();
		String today = new SimpleDateFormat("yyyyMMdd").format(date);
		File file = new File(baseDir + "/alerts/" + id + "/" + today + "_alerts.log");

		if(file.exists()) {
			List<String> lines = FileUtils.readLines(file, "utf-8");
			if(numberOfEntries!=null && numberOfEntries < lines.size()) {
				lines = lines.subList(lines.size()-numberOfEntries, lines.size());
			}
			String alertsLog = StringUtils.join(lines, ',');

			return alertsLog;
		}else {
			return "0";
		}
	}

	public TreeMap<String, String> getIntegrationAlertsCount() throws Exception  {

		TreeMap<String, String> numberOfEntriesList = new TreeMap<String, String>();
		List<TreeMap<String, String>> allProps = super.getFlowConfigurations();
		Iterator<TreeMap<String, String>> it = allProps.iterator();
		while(it.hasNext()){
			TreeMap<String, String> props = it.next();
			String flowId = props.get("id");
			String numberOfEntries =  getFlowAlertsCount(flowId);
			numberOfEntriesList.put(flowId, numberOfEntries);
		}
		return numberOfEntriesList;

	}

	public String getFlowAlertsCount(String id) throws Exception  {

		Date date = new Date();
		String today = new SimpleDateFormat("yyyyMMdd").format(date);
		File file = new File(baseDir + "/alerts/" + id + "/" + today + "_alerts.log");

		if(file.exists()) {
			List<String> lines = FileUtils.readLines(file, "utf-8");
			String numberOfEntries = Integer.toString(lines.size());
			return numberOfEntries;
		}else {
			return "0";
		}
	}

	public String getFlowEventsLog(String id, Integer numberOfEntries) throws Exception  {

		Date date = new Date();
		String today = new SimpleDateFormat("yyyyMMdd").format(date);
		File file = new File(baseDir + "/events/" + id + "/" + today + "_events.log");

		if(file.exists()) {
			List<String> lines = FileUtils.readLines(file, "utf-8");
			if(numberOfEntries!=null && numberOfEntries < lines.size()) {
				lines = lines.subList(lines.size()-numberOfEntries, lines.size());
			}
			String eventLog = StringUtils.join(lines, ',');

			return eventLog;
		}else {
			return "0";
		}
	}


	public String getCamelRouteConfiguration(String id, String mediaType) throws Exception {

		String camelRouteConfiguration = "";

		for (Route route : context.getRoutes()) {
			if(route.getId().equals(id) || route.getId().startsWith(id + "-")) {
				ManagedRouteMBean managedRoute = managed.getManagedRoute(route.getId());
				String xmlConfiguration = managedRoute.dumpRouteAsXml(true);
				xmlConfiguration = xmlConfiguration.replaceAll("\\<\\?xml(.+?)\\?\\>", "").trim();
				camelRouteConfiguration = camelRouteConfiguration + xmlConfiguration;
			}
		}


		if(camelRouteConfiguration.isEmpty()) {
			camelRouteConfiguration = "0";
		}else {
			camelRouteConfiguration = "<routes xmlns=\"http://camel.apache.org/schema/spring\">" +
					camelRouteConfiguration +
					"</routes>";
			if(mediaType.contains("json")) {
				camelRouteConfiguration = DocConverter.convertXmlToJson(camelRouteConfiguration);
			}
		}

		return camelRouteConfiguration;
	}


	//to do
	public String getAllCamelRoutesConfiguration(String mediaType) throws Exception {

		String camelRoutesConfiguration = "{not available yet}";

		return camelRoutesConfiguration;

	}

	private String createKamelet(String name, List<String> parameters) throws IOException {

		String baseName = StringUtils.substringBefore(name, "-");
		String type = StringUtils.substringAfter(name, "-");

		String parametersString = "";
		for(String parameter: parameters){

			if(parameter.contains(", ")){

				String[] splittedParameter = parameter.split("\", \"");

				parametersString = parametersString +
						"      " + splittedParameter[0] + ":\n" +
						"          title: .\n" +
						"          description: .\n" +
						"          type: string\n" +
						"          default: " + splittedParameter[1] + "\n";

			}else if(parameter.contains(",")){

				String[] splittedParameter = parameter.split("\",\"");

				parametersString = parametersString +
						"      " + splittedParameter[0] + ":\n" +
						"          title: .\n" +
						"          description: .\n" +
						"          type: string\n" +
						"          default: " + splittedParameter[1] + "\n";

			}else{
				parametersString = parametersString +
						"      " + parameter + ":\n" +
						"          title: \n" +
						"          description: .\n" +
						"          type: string\n";
			}
		}

		String kamelet = "apiVersion: camel.apache.org/v1alpha1\n" +
				"kind: Kamelet\n" +
				"metadata:\n" +
				"  name: " + name + "\n" +
				"  labels:\n" +
				"    camel.apache.org/kamelet.type: \"" + type + "\"\n" +
				"spec:\n" +
				"  definition:\n" +
				"    title: \"" + baseName + " " + type + "\"\n" +
				"    description: |-\n" +
				"      to do\n" +
				"    type: object\n" +
				"    properties:\n" +
				"      routeconfiguration_id:\n" +
				"        type: string\n" +
				"        default: \"0\"\n" +
				parametersString +
				"  dependencies:\n" +
				"    - \"camel:kamelet\"\n" +
				"  template:\n" +
				"    route-configuration-id: \"{{routeconfiguration_id}}\"\n" +
				"    from:\n" +
				"      uri: \"kamelet:source\"\n" +
				"      steps:\n" +
				"      - to:\n" +
				"        uri: \"kamelet.sink\"";

		return kamelet;
	}


	public String getFlowStats(String flowId, boolean fullStats, boolean includeMetaData, boolean includeSteps, String filter, String mediaType) throws Exception  {

		JSONObject json = new JSONObject();
		JSONObject flow = new JSONObject();
		JSONArray steps = new JSONArray();

		long totalMessages = 0;
		long completedMessages = 0;
		long failedMessages = 0;
		long pendingMessages = 0;
		BigDecimal cpuLoadLastMinute = new BigDecimal("0.00");
		BigDecimal cpuLoadLast5Minutes = new BigDecimal("0.00");
		BigDecimal cpuLoadLast15Minutes = new BigDecimal("0.00");
		long uptimeMillis = 0;
		String uptime = null;
		Date lastFailed = null;
		Date lastCompleted = null;

		List<Route> routes = getRoutesByFlowId(flowId);

		for(Route r : routes){

			String routeId = r.getId();

			if (!filter.isEmpty() && routeId.contains(filter)) {
				continue;
			}

			ManagedRouteMBean route = managed.getManagedRoute(routeId);

			totalMessages += route.getExchangesTotal();
			completedMessages += route.getExchangesCompleted();
			failedMessages += route.getExchangesFailed();
			pendingMessages += route.getExchangesInflight();

			if(fullStats){

				long startTime = System.currentTimeMillis();

				if(uptime==null){
					uptime = route.getUptime();
				}

				if(uptimeMillis==0){
					uptimeMillis = route.getUptimeMillis();
				}

				if(lastFailed==null){
					lastFailed = route.getLastExchangeFailureTimestamp();
				}else{
					Date failure = route.getLastExchangeFailureTimestamp();
					if(failure!=null && failure.after(lastFailed)){
						lastFailed = failure;
					}
				}

				if(lastCompleted==null){
					lastCompleted = route.getLastExchangeCompletedTimestamp();
				}else{
					Date completed = route.getLastExchangeCompletedTimestamp();
					if(completed!=null && completed.after(lastCompleted)){
						lastCompleted = completed;
					}
				}

				cpuLoadLastMinute = cpuLoadLastMinute.add(parseBigDecimal(route.getLoad01()));
				cpuLoadLast5Minutes = cpuLoadLast5Minutes.add(parseBigDecimal(route.getLoad05()));
				cpuLoadLast15Minutes = cpuLoadLast15Minutes.add(parseBigDecimal(route.getLoad15()));

				if(includeSteps){
					JSONObject step = getStepStats(routeId, fullStats);
					steps.put(step);
				}
			}
		}

		flow.put("id",flowId);
		flow.put("total",totalMessages);
		flow.put("completed",completedMessages);
		flow.put("failed",failedMessages);
		flow.put("pending",pendingMessages);

		if(fullStats){
			flow.put("status",getFlowStatus(flowId));
			flow.put("timeout",getTimeout(context));
			flow.put("uptime",uptime);
			flow.put("uptimeMillis",uptimeMillis);
			flow.put("cpuLoadLastMinute",cpuLoadLastMinute);
			flow.put("cpuLoadLast5Minutes",cpuLoadLast5Minutes);
			flow.put("cpuLoadLast15Minutes",cpuLoadLast15Minutes);
			flow.put("lastFailed",lastFailed != null ? lastFailed : "");
			flow.put("lastCompleted",lastCompleted != null ? lastCompleted : "");
		}

		if(includeMetaData){
			TreeMap<String, String> flowProps = getFlowConfiguration(flowId);
			if(flowProps!=null) {
				for (var flowProp : flowProps.entrySet()) {
					if (flowProp.getKey().startsWith("flow") && !flowProp.getKey().endsWith("id")) {
						String key = StringUtils.substringAfter(flowProp.getKey(), "flow.");
						flow.put(key, flowProp.getValue());
					}
				}
			}

		}

		if(includeSteps){
			flow.put("steps",steps);
		}
		json.put("flow",flow);

		String flowStats = json.toString(4);

		if(mediaType.contains("xml")) {
			flowStats = DocConverter.convertJsonToXml(flowStats);
		}

		return flowStats;

	}

	private long getTimeout(CamelContext context) throws MalformedObjectNameException {
		try {
			String managementName = context.getManagementNameStrategy().getName();
			ObjectName objectName = context.getManagementStrategy().getManagementObjectNameStrategy().getObjectNameForCamelContext(managementName, context.getName());

			ManagedCamelContextMBean managedCamelContextMBean = JMX.newMBeanProxy(ManagementFactory.getPlatformMBeanServer(), objectName, ManagedCamelContextMBean.class);
			return managedCamelContextMBean.getTimeout();
		} catch (Exception e) {
			return 0L;
		}
	}

	public String getFlowStepStats(String flowId, String stepid, boolean fullStats, String mediaType) throws Exception {

		String routeid = flowId + "-" + stepid;

		JSONObject json = getStepStats(routeid, fullStats);
		String stepStats = json.toString(4);
		if(mediaType.contains("xml")) {
			stepStats = DocConverter.convertJsonToXml(stepStats);
		}

		return stepStats;
	}

	private JSONObject getStepStats(String routeid, boolean fullStats) throws Exception {

		JSONObject json = new JSONObject();
		JSONObject step = new JSONObject();

		ManagedRouteMBean route = managed.getManagedRoute(routeid);

		String stepStatus = getFlowStatus(routeid);

		step.put("id", routeid);
		step.put("status", stepStatus);

		if(route!=null && flowStatus.equals("started")) {

			if(fullStats){
				String stepUptime = getFlowUptime(routeid);
				String stepUptimeMilliseconds = Long.toString(route.getUptimeMillis());

				step.put("uptime", stepUptime);
				step.put("uptimeMilliseconds", stepUptimeMilliseconds);

				JSONObject load = new JSONObject();

				//String throughput = route.getThroughput();
				String stepLoad01 = route.getLoad01();
				String stepLoad05 = route.getLoad05();
				String stepLoad15 = route.getLoad15();

				//load.put("throughput", throughput);
				load.put("cpuLoadLastMinute", stepLoad01);
				load.put("cpuLoadLast5Minutes", stepLoad05);
				load.put("cpuLoadLast15Minutes", stepLoad15);

				step.put("load", load);
			}

			String statsAsXml = route.dumpStatsAsXml(fullStats);
			String statsAsJson = DocConverter.convertXmlToJson(statsAsXml);
			JSONObject stepStatsObject = new JSONObject(statsAsJson);
			step.put("stats",stepStatsObject.get("stats"));
		}

		json.put("step", step);

		return json;

	}

	public String getStats(String mediaType) throws Exception {

		JSONObject json = new JSONObject();

		ManagedCamelContextMBean managedCamelContext = managed.getManagedCamelContext();

		json.put("camelId",managedCamelContext.getCamelId());
		json.put("camelVersion",managedCamelContext.getCamelVersion());
		json.put("status",managedCamelContext.getState());
		json.put("uptime",managedCamelContext.getUptime());
		json.put("uptimeMillis",managedCamelContext.getUptimeMillis());
		json.put("startedFlows",countFlows("started", "text/plain"));
		json.put("startedSteps",managedCamelContext.getStartedRoutes());
		json.put("exchangesTotal",managedCamelContext.getExchangesTotal());
		json.put("exchangesCompleted",managedCamelContext.getExchangesCompleted());
		json.put("exchangesInflight",managedCamelContext.getExchangesInflight());
		json.put("exchangesFailed",managedCamelContext.getExchangesFailed());
		json.put("cpuLoadLastMinute",managedCamelContext.getLoad01());
		json.put("cpuLoadLast5Minutes",managedCamelContext.getLoad05());
		json.put("cpuLoadLast15Minutes",managedCamelContext.getLoad15());
		json.put("memoryUsage",getMemoryUsage());
		json.put("totalThreads",ManagementFactory.getThreadMXBean().getThreadCount());

		String stats = json.toString(4);
		if(mediaType.contains("xml")) {
			stats = DocConverter.convertJsonToXml(stats);
		}

		return stats;

	}

	private double getMemoryUsage(){

		MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
		MemoryUsage heapMemoryUsage = memoryBean.getHeapMemoryUsage();

		long maxMemory = heapMemoryUsage.getMax(); // Maximum available memory
		long usedMemory = heapMemoryUsage.getUsed(); // Currently used memory

		double memoryUsagePercentage = ((double) usedMemory / maxMemory) * 100;

		DecimalFormat df = new DecimalFormat("#.##");
		memoryUsagePercentage = Double.valueOf(df.format(memoryUsagePercentage));

		return memoryUsagePercentage;

	}

	public String getStepsStats(String mediaType) throws Exception {

		ManagedCamelContextMBean managedCamelContext = managed.getManagedCamelContext();

		String result = managedCamelContext.dumpRoutesStatsAsXml(true,false);

		if(mediaType.contains("json")) {
			result = DocConverter.convertXmlToJson(result);
		}

		return result;

	}


	public String getFlowsStats(String mediaType) throws Exception {

		Set<String> flowIds = new HashSet<String>();

		List<Route> routes = context.getRoutes();

		for(Route route: routes){
			String routeId = route.getId();
			String flowId = StringUtils.substringBefore(routeId,"-");
			if(flowId!=null && !flowId.isEmpty()) {
				flowIds.add(flowId);
			}
		}

		String result = getStatsFromList(flowIds, true, false, false);

		if(mediaType.contains("xml")) {
			result = DocConverter.convertJsonToXml(result);
		}

		return result;

	}

	public String getStatsByFlowIds(String flowIds, String filter, String mediaType) throws Exception {

		String[] values = flowIds.split(",");

		Set<String> flowSet = new HashSet<String>(Arrays.asList(values));

		String result = getStatsFromList(flowSet, filter, true, false,false);

		if(mediaType.contains("xml")) {
			result = DocConverter.convertJsonToXml(result);
		}

		return result;

	}

	private String getStatsFromList(Set<String> flowIds, boolean fullStats, boolean includeMetaData, boolean includeSteps) throws Exception {
		return getStatsFromList(flowIds, "", fullStats, includeMetaData, includeSteps);
	}

	private String getStatsFromList(Set<String> flowIds, String filter, boolean fullStats, boolean includeMetaData, boolean includeSteps) throws Exception {

		JSONArray flows = new JSONArray();

		for(String flowId: flowIds){
			String flowStats = getFlowStats(flowId, fullStats, includeMetaData, includeSteps, filter, "application/json");
			JSONObject flow = new JSONObject(flowStats);
			flows.put(flow);
		}

		String result = flows.toString();

		return result;
	}

	public String getMessages(String mediaType) throws Exception {

		Set<String> flowIds = new HashSet<String>();

		List<Route> routes = context.getRoutes();

		for(Route route: routes){
			String routeId = route.getId();
			String flowId = StringUtils.substringBefore(routeId,"-");
			if(flowId!=null && !flowId.isEmpty()) {
				flowIds.add(flowId);
			}
		}

		String result = getStatsFromList(flowIds, false, false, false);

		if(mediaType.contains("xml")) {
			result = DocConverter.convertJsonToXml(result);
		}

		return result;

	}

	public String getMetrics(String mediaType) throws Exception {

		String integrationStats = "0";
		MetricsRegistryService metricsService = context.hasService(MetricsRegistryService.class);

		if(metricsService!=null) {
			integrationStats = metricsService.dumpStatisticsAsJson();
			if (mediaType.contains("xml")) {
				integrationStats = DocConverter.convertJsonToXml(integrationStats);
			}
		}

		return integrationStats;

	}


	public String getHistoryMetrics(String mediaType) throws Exception {

		String integrationStats = "0";

		MetricsMessageHistoryService historyService = context.hasService(MetricsMessageHistoryService.class);

		if(historyService!=null) {
			integrationStats = historyService.dumpStatisticsAsJson();
			if(mediaType.contains("xml")) {
				integrationStats = DocConverter.convertJsonToXml(integrationStats);
			}
		}

		return integrationStats;

	}

	public String info(String mediaType) throws Exception {

		JSONObject json = new JSONObject();
		JSONObject info = new JSONObject();

		info.put("name",context.getName());
		info.put("version",context.getVersion());
		info.put("startDate", CamelContextHelper.getStartDate(context));
		info.put("startupType",context.getStartupSummaryLevel());
		info.put("uptime",context.getUptime());
		info.put("uptimeMiliseconds", context.getUptime().toMillis());
		info.put("numberOfRunningSteps",context.getRoutesSize());

		json.put("info",info);

		String integrationInfo = json.toString(4);
		if(mediaType.contains("xml")) {
			integrationInfo = DocConverter.convertJsonToXml(integrationInfo);
		}

		return integrationInfo;

	}

	private Set<String> getListOfFlowIds(String filter){

		//get all routes
		List<Route> routes = context.getRoutes();

		Set<String> flowIds = new HashSet<String>();

		//filter flows from routes
		for(Route route: routes){
			String routeId = route.getId();
			String flowId = StringUtils.substringBefore(routeId,"-");
			if(flowId!=null && !flowId.isEmpty()) {
				if (filter != null && !filter.isEmpty()) {
					String status = getFlowStatus(flowId);
					if (status.equalsIgnoreCase(filter)) {
						flowIds.add(flowId);
					}
				}else{
					flowIds.add(flowId);
				}
			}
		}

		return flowIds;

	}

	public String getListOfFlows(String filter, String mediaType) throws Exception {

		Set<String> flowIds = getListOfFlowIds(filter);

		JSONArray flowsArray = new JSONArray();

		for(String flowId: flowIds){
			JSONObject flowObject = new JSONObject();
			flowObject.put("id",flowId);
			flowsArray.put(flowObject);
		}

		String result = flowsArray.toString();

		if(mediaType.contains("xml")) {
			JSONObject flowsObject = new JSONObject();
			JSONObject flowObject = new JSONObject();
			flowObject.put("flow",flowsArray);
			flowsObject.put("flows",flowObject);
			result = DocConverter.convertJsonToXml(flowsObject.toString());
		}

		return result;

	}

	public String getListOfFlowsDetails(String filter, String mediaType) throws Exception {

		Set<String> flowIds = getListOfFlowIds(filter);

		JSONArray flowsArray = new JSONArray();

		for(String flowId: flowIds) {
			JSONObject flowObject = new JSONObject(getFlowInfo(flowId, "application/json"));
			flowsArray.put(flowObject);
		}

		String result = flowsArray.toString();

		if(mediaType.contains("xml")) {
			JSONObject flowsObject = new JSONObject();
			JSONObject flowObject = new JSONObject();
			flowObject.put("flow",flowsArray);
			flowsObject.put("flows",flowObject);
			result = DocConverter.convertJsonToXml(flowsObject.toString());
		}

		return result;

	}

	public String getListOfSoapActions(String url, String mediaType) throws Exception {

		String result;

		Class<?> clazz;
		try {
			clazz = Class.forName("org.assimbly.soap.SoapActionsService");
			Object soapActions =  clazz.getDeclaredConstructor().newInstance();
			Method method = clazz.getDeclaredMethod("getSoapActions", String.class);
			result = (String) method.invoke(soapActions, url);
		} catch (Exception e) {
			log.error("SOAP Actions couldn't be retrieved.", e);
			result = "[]";
		}

		return result;

	}

	public String countFlows(String filter, String mediaType) throws Exception {

		Set<String> flowIds = getListOfFlowIds(filter);

		return Integer.toString(flowIds.size());

	}

	public String countSteps(String filter, String mediaType) throws Exception {

		List<Route> routes = context.getRoutes();

		Set<String> stepIds = new HashSet<String>();

		for(Route route: routes){
			String routeId = route.getId();
			ManagedRouteMBean managedRoute = managed.getManagedRoute(routeId);

			if (filter != null && !filter.isEmpty()) {
				String status = managedRoute.getState();
				if (status.equalsIgnoreCase(filter)) {
					stepIds.add(routeId);
				}
			}else{
				stepIds.add(routeId);
			}
		}

		String numberOfSteps = Integer.toString(stepIds.size());

		return numberOfSteps;

	}

	//Other management tasks

	public TreeMap<String, String> setConnection(TreeMap<String, String> props, String key) throws Exception {
		return new Connection(context, props, key).start();
	}


	public String getDocumentation(String componentType, String mediaType) throws Exception {

		DefaultCamelCatalog catalog = new DefaultCamelCatalog();

		String doc = catalog.componentJSonSchema(componentType);

		if(doc==null || doc.isEmpty()) {
			doc = "Unknown component";
		}

		return doc;
	}

	public String getDocumentationVersion() {

		DefaultCamelCatalog catalog = new DefaultCamelCatalog();

		return catalog.getCatalogVersion();
	}


	public String getComponents(Boolean includeCustomComponents, String mediaType) throws Exception {

		DefaultCamelCatalog catalog = new DefaultCamelCatalog();

		String components = catalog.listComponentsAsJson();

		if(includeCustomComponents){
			URL url = Resources.getResource("custom-steps.json");
			String customComponent = Resources.toString(url, StandardCharsets.UTF_8);
			components = JsonHelper.mergeJsonArray(components,customComponent);
		}

		if(mediaType.contains("xml")) {
			components = DocConverter.convertJsonToXml(components);
		}

		return components;
	}

	public String getComponentSchema(String componentType, String mediaType) throws Exception {

		DefaultCamelCatalog catalog = new DefaultCamelCatalog();

		String schema = catalog.componentJSonSchema(componentType);

		if(schema==null || schema.isEmpty()) {
			URL url = Resources.getResource("custom-steps-parameters.json");
			String customSchemas = Resources.toString(url, StandardCharsets.UTF_8);
			JSONArray jsonArray = new JSONArray(customSchemas);
			for(int i=0;i<jsonArray.length();i++)
			{
				JSONObject components = jsonArray.getJSONObject(i);
				JSONObject component = components.getJSONObject("component");
				String name = component.getString("name");
				if(name.equalsIgnoreCase(componentType)){
					schema = components.toString();
					break;
				}
			}
		}

		if(schema==null || schema.isEmpty()) {
			schema = "Unknown component";
		}else if(mediaType.contains("xml")) {
			schema = DocConverter.convertJsonToXml(schema);
		}

		return schema;
	}

	@Override
	public String getComponentParameters(String componentType, String mediaType) throws Exception {

		DefaultCamelCatalog catalog = new DefaultCamelCatalog();

		String parameters = catalog.componentJSonSchema(componentType);

		if(parameters==null || parameters.isEmpty()) {
			parameters = "Unknown component";
		}else if(mediaType.contains("xml")) {
			parameters = DocConverter.convertJsonToXml(parameters);
		}

		return parameters;
	}

	public String validateFlow(String uri) {

		DefaultCamelCatalog catalog = new DefaultCamelCatalog();

		EndpointValidationResult valid = catalog.validateEndpointProperties(uri);

		if(valid.hasErrors()){
			return "invalid: " + valid.summaryErrorMessage(false);
		}else {
			return "valid";
		}

	}

	public String resolveDependency(String scheme) throws Exception {

		DefaultCamelCatalog catalog = new DefaultCamelCatalog();
		String jsonString = catalog.componentJSonSchema(scheme);

		if(jsonString == null || jsonString.isEmpty()){
			log.info("Unknown scheme: " + scheme);
			return null;
		}
		JSONObject componentSchema = new JSONObject(jsonString);
		JSONObject component = componentSchema.getJSONObject("component");

		String groupId = component.getString("groupId");
		String artifactId = component.getString("artifactId");
		String version = catalog.getCatalogVersion();

		String dependency = groupId + ":" + artifactId + ":" + version;
		String result;

		try {
			List<Class> classes = resolveMavenDependency(groupId, artifactId, version);
			Component camelComponent = getComponent(classes, scheme);
			context.addComponent(scheme, camelComponent);
			result = "Dependency " + dependency + " resolved";
		} catch (Exception e) {
			result = "Dependency " + dependency + " resolved failed. Error message: "  + e.getMessage();
		}

		return result;

	}


	public List<Class> resolveMavenDependency(String groupId, String artifactId, String version) throws Exception {

		DependencyUtil dependencyUtil = new DependencyUtil();
		List<Path> paths = dependencyUtil.resolveDependency(groupId, artifactId, version);
		List<Class> classes = dependencyUtil.loadDependency(paths);

		return classes;

	}

	public Component getComponent(List<Class> classes, String scheme) throws Exception {

		Component component = null;
		for(Class classToLoad: classes){
			String className = classToLoad.getName().toLowerCase();
			if(className.endsWith(scheme + "component")){
				Object object =  classToLoad.newInstance();
				component = (Component) object;
			}
		}

		return component;
	}


	public  CamelContext getContext() {
		return context;
	}

	public ProducerTemplate getProducerTemplate() {
		return context.createProducerTemplate();
	}

	public ConsumerTemplate getConsumerTemplate() {
		return context.createConsumerTemplate();
	}

	public void send(Object messageBody, ProducerTemplate template) {
		template.sendBody(messageBody);
	}

	public void sendWithHeaders(Object messageBody, TreeMap<String, Object> messageHeaders, ProducerTemplate template) {
		template.sendBodyAndHeaders(messageBody, messageHeaders);
	}


	public void send(String uri,Object messageBody, Integer numberOfTimes) {

		ProducerTemplate template = context.createProducerTemplate();

		if(numberOfTimes.equals(1)){
			log.info("Sending " + numberOfTimes + " message to " + uri);
			template.sendBody(uri, messageBody);
		}else{
			log.info("Sending " + numberOfTimes + " messages to " + uri);
			IntStream.range(0, numberOfTimes).forEach(i -> template.sendBody(uri, messageBody));
		}
	}

	public void sendWithHeaders(String uri, Object messageBody, TreeMap<String, Object> messageHeaders, Integer numberOfTimes) {

		ProducerTemplate template = context.createProducerTemplate();

		Exchange exchange = new DefaultExchange(context);
		exchange.getIn().setBody(messageBody);
		exchange = setHeaders(exchange, messageHeaders);

		if(numberOfTimes.equals(1)){
			log.info("Sending " + numberOfTimes + " message to " + uri);
			template.send(uri,exchange);
		}else{
			log.info("Sending " + numberOfTimes + " messages to " + uri);
			Exchange finalExchange = exchange;
			IntStream.range(0, numberOfTimes).forEach(i -> template.send(uri, finalExchange));
		}

	}

	public String sendRequest(String uri,Object messageBody) {

		ProducerTemplate template = context.createProducerTemplate();

		log.info("Sending request message to " + uri);

		return template.requestBody(uri, messageBody,String.class);
	}

	public String sendRequestWithHeaders(String uri, Object messageBody, TreeMap<String, Object> messageHeaders) {

		ProducerTemplate template = context.createProducerTemplate();

		Exchange exchange = new DefaultExchange(context);
		exchange.getIn().setBody(messageBody);
		exchange = setHeaders(exchange, messageHeaders);
		exchange.setPattern(ExchangePattern.InOut);

		log.info("Sending request message to " + uri);
		Exchange result = template.send(uri,exchange);

		return result.getMessage().getBody(String.class);
	}

	public Exchange setHeaders(Exchange exchange, TreeMap<String, Object> messageHeaders){
		for(Map.Entry<String,Object> messageHeader : messageHeaders.entrySet()) {

			String key = messageHeader.getKey();
			String value = StringUtils.substringBetween(messageHeader.getValue().toString(),"(",")");
			String language = StringUtils.substringBefore(messageHeader.getValue().toString(),"(");
			String result;

			if(value.startsWith("constant")) {
				exchange.getIn().setHeader(key,value);
			}else if(value.startsWith("xpath")){
				XPathFactory fac = new net.sf.saxon.xpath.XPathFactoryImpl();
				result = XPathBuilder.xpath(key).factory(fac).evaluate(exchange, String.class);
				exchange.getIn().setHeader(key,result);
			}else{
				Language resolvedLanguage = exchange.getContext().resolveLanguage(language);
				Expression expression = resolvedLanguage.createExpression(value);
				result = expression.evaluate(exchange, String.class);
				exchange.getIn().setHeader(key,result);
			}

		}

		return exchange;
	}


	public Certificate[] getCertificates(String url) {

		Certificate[] certificates = new Certificate[0];

		try {
			CertificatesUtil util = new CertificatesUtil();
			certificates = util.downloadCertificates(url);
		} catch (Exception e) {
			log.error("Start certificates for url " + url + " failed.",e);
		}
		return certificates;
	}

	public Certificate getCertificateFromKeystore(String keystoreName, String keystorePassword, String certificateName) {
		String keystorePath = baseDir + SEP + SECURITY_PATH + SEP + keystoreName;
		CertificatesUtil util = new CertificatesUtil();
		return util.getCertificate(keystorePath, keystorePassword, certificateName);
	}

	public void setCertificatesInKeystore(String keystoreName, String keystorePassword, String url) {

		try {
			CertificatesUtil util = new CertificatesUtil();
			Certificate[] certificates = util.downloadCertificates(url);
			String keystorePath = baseDir + SEP + SECURITY_PATH + SEP + keystoreName;
			util.importCertificates(keystorePath, keystorePassword, certificates);
		} catch (Exception e) {
			log.error("Set certificates for url " + url + " failed.",e);
		}
	}

	public String importCertificateInKeystore(String keystoreName, String keystorePassword, String certificateName, Certificate certificate) {

		CertificatesUtil util = new CertificatesUtil();

		String keystorePath = baseDir + SEP + SECURITY_PATH + SEP + keystoreName;

		File file = new File(keystorePath);

		String result;

		if(file.exists()) {
			result = util.importCertificate(keystorePath, keystorePassword, certificateName,certificate);
		}else{
			result = "Keystore doesn't exist";
		}

		return result;

	}


	public Map<String,Certificate> importCertificatesInKeystore(String keystoreName, String keystorePassword, Certificate[] certificates) throws Exception {

		CertificatesUtil util = new CertificatesUtil();

		String keystorePath = baseDir + SEP + SECURITY_PATH + SEP + keystoreName;

		File file = new File(keystorePath);

		if(file.exists()) {
			return util.importCertificates(keystorePath, keystorePassword, certificates);
		}else{
			throw new KeyStoreException("Keystore " + keystoreName + "doesn't exist");
		}

	}

	public Map<String,Certificate> importP12CertificateInKeystore(String keystoreName, String keystorePassword, String p12Certificate, String p12Password) throws Exception {

		CertificatesUtil util = new CertificatesUtil();

		String keystorePath = baseDir + SEP + SECURITY_PATH + SEP + keystoreName;
		return util.importP12Certificate(keystorePath, keystorePassword, p12Certificate, p12Password);

	}

	public void deleteCertificateInKeystore(String keystoreName, String keystorePassword, String certificateName) {

		String keystorePath = baseDir + SEP + SECURITY_PATH + SEP + keystoreName;

		CertificatesUtil util = new CertificatesUtil();
		util.deleteCertificate(keystorePath, keystorePassword, certificateName);
	}

	@Override
	public ValidationErrorMessage validateCron(String cronExpression) {
		CronValidator cronValidator = new CronValidator();
		return cronValidator.validate(cronExpression);
	}

	@Override
	public HttpsCertificateValidator.ValidationResult validateCertificate(String httpsUrl) {
		HttpsCertificateValidator httpsCertificateValidator = new HttpsCertificateValidator();
		try {
			List<String> urlList = new ArrayList<>();
			urlList.add(httpsUrl);
			httpsCertificateValidator.addHttpsCertificatesToTrustStore(urlList);
		} catch (Exception e) {
			log.error("Error to add certificate: " + e.getMessage());
		}
		return httpsCertificateValidator.validate(httpsUrl);
	}

	@Override
	public ValidationErrorMessage validateUrl(String url) {
		UrlValidator urlValidator = new UrlValidator();
		return urlValidator.validate(url);
	}

	@Override
	public List<ValidationErrorMessage> validateExpressions(List<org.assimbly.dil.validation.beans.Expression> expressions, boolean isPredicate) {
		ExpressionsValidator expressionValidator = new ExpressionsValidator();
		return expressionValidator.validate(expressions, isPredicate);
	}

	@Override
	public ValidationErrorMessage validateFtp(FtpSettings ftpSettings) {
		FtpValidator ftpValidator = new FtpValidator();
		return ftpValidator.validate(ftpSettings);
	}

	@Override
	public AbstractMap.SimpleEntry validateRegex(Regex regex) {
		RegexValidator regexValidator = new RegexValidator();
		return regexValidator.validate(regex);
	}

	@Override
	public EvaluationResponse validateScript(EvaluationRequest scriptRequest) {
		ScriptValidator scriptValidator = new ScriptValidator();
		return scriptValidator.validate(scriptRequest);
	}

	@Override
	public List<ValidationErrorMessage> validateXslt(String url, String xsltBody) {
		XsltValidator xsltValidator = new XsltValidator();
		return xsltValidator.validate(url, xsltBody);
	}

	public void setEncryptionProperties(Properties encryptionProperties) {
		this.encryptionProperties = encryptionProperties;
		setEncryptedPropertiesComponent();
	}

	public EncryptionUtil getEncryptionUtil() {
		return new EncryptionUtil(encryptionProperties.getProperty("password"), encryptionProperties.getProperty("algorithm"));
	}

	private void setEncryptedPropertiesComponent() {
		EncryptionUtil encryptionUtil = getEncryptionUtil();
		EncryptableProperties initialProperties = new EncryptableProperties(encryptionUtil.getTextEncryptor());
		PropertiesComponent propertiesComponent = new PropertiesComponent();
		propertiesComponent.setInitialProperties(initialProperties);
		context.setPropertiesComponent(propertiesComponent);
	}

	private void setSSLContext() throws Exception {

		String baseDir2 = FilenameUtils.separatorsToUnix(baseDir);

		File securityPath = new File(baseDir + SEP + SECURITY_PATH + SEP);

		if (!securityPath.exists()) {
			boolean securityPathCreated = securityPath.mkdirs();
			if(!securityPathCreated){
				throw new Exception("Directory: " + securityPath.getAbsolutePath() + " cannot be create to store keystore files");
			}
		}

		String keyStorePath = baseDir2 + SEP + SECURITY_PATH + SEP + KEYSTORE_FILE;
		String trustStorePath = baseDir2 + SEP + SECURITY_PATH + SEP + TRUSTSTORE_FILE;

		SSLConfiguration sslConfiguration = new SSLConfiguration();

		SSLContextParameters sslContextParameters = sslConfiguration.createSSLContextParameters(keyStorePath, getKeystorePassword(), trustStorePath, getKeystorePassword());

		SSLContextParameters sslContextParametersKeystoreOnly = sslConfiguration.createSSLContextParameters(keyStorePath, getKeystorePassword(), null, null);

		SSLContextParameters sslContextParametersTruststoreOnly = sslConfiguration.createSSLContextParameters(null, null, trustStorePath, getKeystorePassword());

		registry.bind("default", sslContextParameters);
		registry.bind("sslContext", sslContextParameters);
		registry.bind("keystore", sslContextParametersKeystoreOnly);
		registry.bind("truststore", sslContextParametersTruststoreOnly);


		// use default
		//JettyHttpComponent jetty = context.getComponent("jetty", JettyHttpComponent.class);

		//JettyHttpComponent12 jetty = context.getComponent("jetty", org.assimbly.jetty.JettyHttpComponent12.class);
		//context.
		//jetty.setSslContextParameters(sslContextParameters);

		try {
			SSLContext sslContext = sslContextParameters.createSSLContext(context);
			sslContext.createSSLEngine();
		}catch (Exception e){
			log.error("Can't set SSL context for certificate keystore. TLS/SSL certificates are not available. Reason: " + e.getMessage());
		}

		String[] sslComponents = {"ftps", "https", "imaps", "kafka", "jetty", "netty", "netty-http", "smtps", "vertx-http"};

		sslConfiguration.setUseGlobalSslContextParameters(context, sslComponents);

		//sslConfiguration.initTrustStoresForHttpsCertificateValidator(keyStorePath, getKeystorePassword(), trustStorePath, getKeystorePassword());

	}

	/**
	 * This method returns a List of all Routes of a flow given the flowID, or a single route (from or to) given a routeID.
	 * @param id The flowID or routeID
	 * @return A List of Routes
	 */
	private List<Route> getRoutesByFlowId(String id){
		return context.getRoutes().stream().filter(r -> r.getId().startsWith(id)).collect(Collectors.toList());
	}

	private String getKeystorePassword() {
		String keystorePwd = System.getenv(KEYSTORE_PWD);
		if(StringUtils.isEmpty(keystorePwd)) {
			return "supersecret";
		}

		return keystorePwd;
	}

	public static BigDecimal parseBigDecimal(String value) {
		if (value == null || value.isEmpty()) {
			return BigDecimal.ZERO;  // or handle as needed
		}
		return new BigDecimal(value);
	}

	private static int getEnvironmentVarAsInteger(String envName, int defaultValue){
		int value = defaultValue;
		if (envName != null && !envName.isEmpty()) {
			try {
				value = Integer.parseInt(envName);
			} catch (NumberFormatException e) {
				System.err.println("Invalid value for " + envName + ": " + value);
			}
		}
		return value;
	}

	public FlowLoaderReport getFlowLoaderReport(String id) {
		return flowLoaderReportMap.computeIfAbsent(id, key -> new FlowLoaderReport());
	}

	public void clearFlowLoaderReport(String id) {
		flowLoaderReportMap.remove(id);
	}

}