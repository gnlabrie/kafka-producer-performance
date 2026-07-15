package com.kafkaproducertest.config;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;

/** Typed configuration for the Kafka Producer Test Tool. */
@ConfigurationProperties(prefix = "kafka-producer-test")
public class KafkaProducerTestProperties {
  private ExecutionMode mode = ExecutionMode.RUN;
  private Run run = new Run();
  private Topic topic = new Topic();
  private Connection connection = new Connection();
  private Security security = new Security();
  private Key key = new Key();
  private Payload payload = new Payload();
  private Headers headers = new Headers();
  private Producer producer = new Producer();
  private Statistics statistics = new Statistics();
  private Summary summary = new Summary();

  public ExecutionMode getMode() {
    return mode;
  }

  public void setMode(final ExecutionMode value) {
    mode = value;
  }

  public Run getRun() {
    return run;
  }

  public void setRun(final Run value) {
    run = value;
  }

  /**
   * Returns topic provisioning settings used when creating a missing topic.
   *
   * @return topic create-if-absent configuration
   */
  public Topic getTopic() {
    return topic;
  }

  /**
   * Sets topic provisioning settings.
   *
   * @param value topic configuration
   */
  public void setTopic(final Topic value) {
    topic = value;
  }

  public Connection getConnection() {
    return connection;
  }

  public void setConnection(final Connection value) {
    connection = value;
  }

  public Security getSecurity() {
    return security;
  }

  public void setSecurity(final Security value) {
    security = value;
  }

  public Key getKey() {
    return key;
  }

  public void setKey(final Key value) {
    key = value;
  }

  public Payload getPayload() {
    return payload;
  }

  public void setPayload(final Payload value) {
    payload = value;
  }

  public Headers getHeaders() {
    return headers;
  }

  public void setHeaders(final Headers value) {
    headers = value;
  }

  public Producer getProducer() {
    return producer;
  }

  public void setProducer(final Producer value) {
    producer = value;
  }

  public Statistics getStatistics() {
    return statistics;
  }

  public void setStatistics(final Statistics value) {
    statistics = value;
  }

  public Summary getSummary() {
    return summary;
  }

  public void setSummary(final Summary value) {
    summary = value;
  }

  /**
   * Topic provisioning settings applied when {@code create-if-absent} is enabled. The topic name
   * remains {@code run.topic}.
   */
  public static class Topic {
    private boolean createIfAbsent;
    private int partitions = 1;
    private short replicationFactor = 1;
    private Map<String, String> configs = new LinkedHashMap<>();

    /**
     * Returns whether the tool should create the topic when it does not exist.
     *
     * @return true when AdminClient create is requested
     */
    public boolean isCreateIfAbsent() {
      return createIfAbsent;
    }

    /**
     * Sets whether missing topics should be created before the run.
     *
     * @param value create-if-absent flag
     */
    public void setCreateIfAbsent(final boolean value) {
      createIfAbsent = value;
    }

    /**
     * Returns the partition count used when creating the topic.
     *
     * @return partition count
     */
    public int getPartitions() {
      return partitions;
    }

    /**
     * Sets the partition count used when creating the topic.
     *
     * @param value partition count
     */
    public void setPartitions(final int value) {
      partitions = value;
    }

    /**
     * Returns the replication factor used when creating the topic.
     *
     * @return replication factor
     */
    public short getReplicationFactor() {
      return replicationFactor;
    }

    /**
     * Sets the replication factor used when creating the topic.
     *
     * @param value replication factor
     */
    public void setReplicationFactor(final short value) {
      replicationFactor = value;
    }

    /**
     * Returns optional Kafka topic configuration entries applied at create time.
     *
     * @return topic config map
     */
    public Map<String, String> getConfigs() {
      return configs;
    }

    /**
     * Sets optional Kafka topic configuration entries applied at create time.
     *
     * @param value topic config map
     */
    public void setConfigs(final Map<String, String> value) {
      configs = value;
    }
  }

  /** Run lifecycle configuration. */
  public static class Run {
    private String name = "kafka-producer-test";
    private String description;
    private UUID runId;
    private String topic;
    private Long messageCount;
    private Duration duration;
    private Double messagesPerSecond;
    private Duration shutdownTimeout = Duration.ofSeconds(30);
    private boolean failFast = true;
    private int maximumErrors = 1;

    public String getName() {
      return name;
    }

    public void setName(final String value) {
      name = value;
    }

    public String getDescription() {
      return description;
    }

    public void setDescription(final String value) {
      description = value;
    }

    public UUID getRunId() {
      return runId;
    }

    public void setRunId(final UUID value) {
      runId = value;
    }

    public String getTopic() {
      return topic;
    }

    public void setTopic(final String value) {
      topic = value;
    }

    public Long getMessageCount() {
      return messageCount;
    }

    public void setMessageCount(final Long value) {
      messageCount = value;
    }

    public Duration getDuration() {
      return duration;
    }

    public void setDuration(final Duration value) {
      duration = value;
    }

    public Double getMessagesPerSecond() {
      return messagesPerSecond;
    }

    public void setMessagesPerSecond(final Double value) {
      messagesPerSecond = value;
    }

    public Duration getShutdownTimeout() {
      return shutdownTimeout;
    }

    public void setShutdownTimeout(final Duration value) {
      shutdownTimeout = value;
    }

    public boolean isFailFast() {
      return failFast;
    }

    public void setFailFast(final boolean value) {
      failFast = value;
    }

    public int getMaximumErrors() {
      return maximumErrors;
    }

    public void setMaximumErrors(final int value) {
      maximumErrors = value;
    }
  }

  /** Kafka connectivity configuration. */
  public static class Connection {
    private List<String> bootstrapServers = new ArrayList<>();
    private String clientId;

    public List<String> getBootstrapServers() {
      return bootstrapServers;
    }

    public void setBootstrapServers(final List<String> value) {
      bootstrapServers = value;
    }

    public String getClientId() {
      return clientId;
    }

    public void setClientId(final String value) {
      clientId = value;
    }
  }

  /** Kafka security configuration. */
  public static class Security {
    private AuthenticationMode mode = AuthenticationMode.PLAINTEXT;
    private Path truststorePath;
    private String truststorePassword;
    private String truststoreType = "PKCS12";
    private Path keystorePath;
    private String keystorePassword;
    private String keystoreType = "PKCS12";
    private String keyPassword;
    private String endpointIdentificationAlgorithm = "https";
    private String gssapiServiceName = "kafka";
    private String gssapiPrincipal;
    private Path gssapiKeytab;
    private boolean gssapiUseTicketCache;
    private GssapiLoginMode gssapiLoginMode = GssapiLoginMode.TICKET_CACHE;
    private String jaasConfig;

    public AuthenticationMode getMode() {
      return mode;
    }

    public void setMode(final AuthenticationMode value) {
      mode = value;
    }

    public Path getTruststorePath() {
      return truststorePath;
    }

    public void setTruststorePath(final Path value) {
      truststorePath = value;
    }

    public String getTruststorePassword() {
      return truststorePassword;
    }

    public void setTruststorePassword(final String value) {
      truststorePassword = value;
    }

    public String getTruststoreType() {
      return truststoreType;
    }

    public void setTruststoreType(final String value) {
      truststoreType = value;
    }

    public Path getKeystorePath() {
      return keystorePath;
    }

    public void setKeystorePath(final Path value) {
      keystorePath = value;
    }

    public String getKeystorePassword() {
      return keystorePassword;
    }

    public void setKeystorePassword(final String value) {
      keystorePassword = value;
    }

    public String getKeystoreType() {
      return keystoreType;
    }

    public void setKeystoreType(final String value) {
      keystoreType = value;
    }

    public String getKeyPassword() {
      return keyPassword;
    }

    public void setKeyPassword(final String value) {
      keyPassword = value;
    }

    public String getEndpointIdentificationAlgorithm() {
      return endpointIdentificationAlgorithm;
    }

    public void setEndpointIdentificationAlgorithm(final String value) {
      endpointIdentificationAlgorithm = value;
    }

    public String getGssapiServiceName() {
      return gssapiServiceName;
    }

    public void setGssapiServiceName(final String value) {
      gssapiServiceName = value;
    }

    public String getGssapiPrincipal() {
      return gssapiPrincipal;
    }

    public void setGssapiPrincipal(final String value) {
      gssapiPrincipal = value;
    }

    public Path getGssapiKeytab() {
      return gssapiKeytab;
    }

    public void setGssapiKeytab(final Path value) {
      gssapiKeytab = value;
    }

    public boolean isGssapiUseTicketCache() {
      return gssapiUseTicketCache;
    }

    public void setGssapiUseTicketCache(final boolean value) {
      gssapiUseTicketCache = value;
    }

    public GssapiLoginMode getGssapiLoginMode() {
      return gssapiLoginMode;
    }

    public void setGssapiLoginMode(final GssapiLoginMode value) {
      gssapiLoginMode = value;
    }

    public String getJaasConfig() {
      return jaasConfig;
    }

    public void setJaasConfig(final String value) {
      jaasConfig = value;
    }
  }

  /** Key generation configuration. */
  public static class Key {
    private KeyMode mode = KeyMode.NONE;
    private String fixedValue;
    private Integer length = 16;

    public KeyMode getMode() {
      return mode;
    }

    public void setMode(final KeyMode value) {
      mode = value;
    }

    public String getFixedValue() {
      return fixedValue;
    }

    public void setFixedValue(final String value) {
      fixedValue = value;
    }

    public Integer getLength() {
      return length;
    }

    public void setLength(final Integer value) {
      length = value;
    }
  }

  /** Payload generation configuration. */
  public static class Payload {
    private PayloadMode mode = PayloadMode.FIXED;
    private ContentMode contentMode = ContentMode.RANDOM;
    private DataSize length = DataSize.ofBytes(1024);
    private DataSize minimumLength;
    private DataSize maximumLength;
    private String text;
    private Long seed;

    public PayloadMode getMode() {
      return mode;
    }

    public void setMode(final PayloadMode value) {
      mode = value;
    }

    public ContentMode getContentMode() {
      return contentMode;
    }

    public void setContentMode(final ContentMode value) {
      contentMode = value;
    }

    public DataSize getLength() {
      return length;
    }

    public void setLength(final DataSize value) {
      length = value;
    }

    public DataSize getMinimumLength() {
      return minimumLength;
    }

    public void setMinimumLength(final DataSize value) {
      minimumLength = value;
    }

    public DataSize getMaximumLength() {
      return maximumLength;
    }

    public void setMaximumLength(final DataSize value) {
      maximumLength = value;
    }

    public String getText() {
      return text;
    }

    public void setText(final String value) {
      text = value;
    }

    public Long getSeed() {
      return seed;
    }

    public void setSeed(final Long value) {
      seed = value;
    }
  }

  /** Record header configuration. */
  public static class Headers {
    private boolean includeDefaultHeaders = true;
    private Map<String, String> custom = new LinkedHashMap<>();

    public boolean isIncludeDefaultHeaders() {
      return includeDefaultHeaders;
    }

    public void setIncludeDefaultHeaders(final boolean value) {
      includeDefaultHeaders = value;
    }

    public Map<String, String> getCustom() {
      return custom;
    }

    public void setCustom(final Map<String, String> value) {
      custom = value;
    }
  }

  /** Typed Kafka producer configuration. */
  public static class Producer {
    private String acks = "all";
    private CompressionType compressionType = CompressionType.NONE;
    private Duration linger = Duration.ZERO;
    private DataSize batchSize = DataSize.ofBytes(16384);
    private DataSize bufferMemory = DataSize.ofBytes(33554432);
    private DataSize maxRequestSize = DataSize.ofBytes(1048576);
    private Integer retries = Integer.MAX_VALUE;
    private Duration requestTimeout = Duration.ofSeconds(30);
    private Duration deliveryTimeout = Duration.ofMinutes(2);
    private Duration maxBlock = Duration.ofSeconds(60);
    private Integer maxInFlightRequests = 5;
    private Boolean enableIdempotence = true;
    private String clientDnsLookup;
    private Duration metadataMaxAge;
    private DataSize sendBuffer;
    private DataSize receiveBuffer;
    private Map<String, Object> properties = new LinkedHashMap<>();

    public String getAcks() {
      return acks;
    }

    public void setAcks(final String value) {
      acks = value;
    }

    public CompressionType getCompressionType() {
      return compressionType;
    }

    public void setCompressionType(final CompressionType value) {
      compressionType = value;
    }

    public Duration getLinger() {
      return linger;
    }

    public void setLinger(final Duration value) {
      linger = value;
    }

    public DataSize getBatchSize() {
      return batchSize;
    }

    public void setBatchSize(final DataSize value) {
      batchSize = value;
    }

    public DataSize getBufferMemory() {
      return bufferMemory;
    }

    public void setBufferMemory(final DataSize value) {
      bufferMemory = value;
    }

    public DataSize getMaxRequestSize() {
      return maxRequestSize;
    }

    public void setMaxRequestSize(final DataSize value) {
      maxRequestSize = value;
    }

    public Integer getRetries() {
      return retries;
    }

    public void setRetries(final Integer value) {
      retries = value;
    }

    public Duration getRequestTimeout() {
      return requestTimeout;
    }

    public void setRequestTimeout(final Duration value) {
      requestTimeout = value;
    }

    public Duration getDeliveryTimeout() {
      return deliveryTimeout;
    }

    public void setDeliveryTimeout(final Duration value) {
      deliveryTimeout = value;
    }

    public Duration getMaxBlock() {
      return maxBlock;
    }

    public void setMaxBlock(final Duration value) {
      maxBlock = value;
    }

    public Integer getMaxInFlightRequests() {
      return maxInFlightRequests;
    }

    public void setMaxInFlightRequests(final Integer value) {
      maxInFlightRequests = value;
    }

    public Boolean getEnableIdempotence() {
      return enableIdempotence;
    }

    public void setEnableIdempotence(final Boolean value) {
      enableIdempotence = value;
    }

    public String getClientDnsLookup() {
      return clientDnsLookup;
    }

    public void setClientDnsLookup(final String value) {
      clientDnsLookup = value;
    }

    public Duration getMetadataMaxAge() {
      return metadataMaxAge;
    }

    public void setMetadataMaxAge(final Duration value) {
      metadataMaxAge = value;
    }

    public DataSize getSendBuffer() {
      return sendBuffer;
    }

    public void setSendBuffer(final DataSize value) {
      sendBuffer = value;
    }

    public DataSize getReceiveBuffer() {
      return receiveBuffer;
    }

    public void setReceiveBuffer(final DataSize value) {
      receiveBuffer = value;
    }

    public Map<String, Object> getProperties() {
      return properties;
    }

    public void setProperties(final Map<String, Object> value) {
      properties = value;
    }
  }

  /** Detailed statistics output configuration. */
  public static class Statistics {
    private boolean enabled = true;
    private Path outputDirectory = Path.of("output");
    private String fileNamePrefix = "statistics";
    private int queueCapacity = 10000;
    private Duration flushInterval = Duration.ofSeconds(1);
    private boolean stopOnQueueOverflow = true;

    public boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(final boolean value) {
      enabled = value;
    }

    public Path getOutputDirectory() {
      return outputDirectory;
    }

    public void setOutputDirectory(final Path value) {
      outputDirectory = value;
    }

    public String getFileNamePrefix() {
      return fileNamePrefix;
    }

    public void setFileNamePrefix(final String value) {
      fileNamePrefix = value;
    }

    public int getQueueCapacity() {
      return queueCapacity;
    }

    public void setQueueCapacity(final int value) {
      queueCapacity = value;
    }

    public Duration getFlushInterval() {
      return flushInterval;
    }

    public void setFlushInterval(final Duration value) {
      flushInterval = value;
    }

    public boolean isStopOnQueueOverflow() {
      return stopOnQueueOverflow;
    }

    public void setStopOnQueueOverflow(final boolean value) {
      stopOnQueueOverflow = value;
    }
  }

  /** Summary output configuration. */
  public static class Summary {
    private boolean writeJson = true;
    private boolean writeYaml;
    private Path outputDirectory = Path.of("output");
    private String fileNamePrefix = "summary";
    private boolean printConsole = true;

    public boolean isWriteJson() {
      return writeJson;
    }

    public void setWriteJson(final boolean value) {
      writeJson = value;
    }

    public boolean isWriteYaml() {
      return writeYaml;
    }

    public void setWriteYaml(final boolean value) {
      writeYaml = value;
    }

    public Path getOutputDirectory() {
      return outputDirectory;
    }

    public void setOutputDirectory(final Path value) {
      outputDirectory = value;
    }

    public String getFileNamePrefix() {
      return fileNamePrefix;
    }

    public void setFileNamePrefix(final String value) {
      fileNamePrefix = value;
    }

    public boolean isPrintConsole() {
      return printConsole;
    }

    public void setPrintConsole(final boolean value) {
      printConsole = value;
    }
  }
}
