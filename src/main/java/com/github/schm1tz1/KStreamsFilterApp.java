package com.github.schm1tz1;

import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * Runnable Kafka Streams filtering application with command-line argument parsing.
 *
 * <p>Reads a configuration file, optionally enables monitoring interceptors, and starts a
 * {@link StreamsPipeline} that filters Kafka record headers based on a regular expression.
 */
@CommandLine.Command(
    name = "KStreamsFilterApp",
    version = "KStreamsFilterApp 0.1",
    description = "Kafka Streams template app.",
    mixinStandardHelpOptions = true)
public class KStreamsFilterApp implements Runnable {
  static final Logger logger = LoggerFactory.getLogger(KStreamsFilterApp.class);

  @CommandLine.Option(
      names = {"-c", "--config-file"},
      description = "If provided, content will be added to the properties")
  protected static String configFile = null;

  @CommandLine.Option(
      names = {"-C", "--additional-config-file"},
      description =
          "If provided, will add additional configurations e.g. for overrides and business logic")
  protected static String additionalConfigFile = null;

  @CommandLine.Option(
      names = {"--enable-monitoring-interceptor"},
      description = "Enable MonitoringInterceptors (for Control Center)")
  protected boolean monitoringInterceptors = false;

  /**
   * Application entry point.
   *
   * @param args command-line arguments forwarded to the picocli command parser
   * @throws Exception if the command execution fails unexpectedly
   */
  public static void main(String[] args) throws Exception {
    int returnCode = new CommandLine(new KStreamsFilterApp()).execute(args);
    System.exit(returnCode);
  }

  /** Configures and starts the Kafka Streams header-filtering pipeline. */
  @Override
  public void run() {
    Properties streamProperties =
        PipelineConfigTools.configureStreamsProperties(configFile, additionalConfigFile);
    if (monitoringInterceptors) {
      PipelineConfigTools.addMonitoringInterceptorConfig(streamProperties);
    }
    StreamsPipeline eventFilterPipeline = new StreamsPipeline(streamProperties);
    eventFilterPipeline.run();
  }
}
