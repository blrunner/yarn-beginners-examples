/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wikibooks.hadoop.yarn.examples;

import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.*;

/**
 * Client for HelloYarn submission to YARN.
 *
 * <p> This client allows an application master to be launched that in turn would run
 * the provided the HelloYarn on a set of containers. </p>
 *
 * <p>This client is meant to act as an example on how to write yarn-based applications. </p>
 *
 * <p> To submit an application, a client first needs to connect to the <code>ResourceManager</code>
 * aka ApplicationsManager or ASM via the {@link org.apache.hadoop.yarn.api.ApplicationClientProtocol}. The {@link org.apache.hadoop.yarn.api.ApplicationClientProtocol}
 * provides a way for the client to get access to cluster information and to request for a
 * new {@link org.apache.hadoop.yarn.api.records.ApplicationId}. <p>
 *
 * <p> For the actual job submission, the client first has to create an {@link org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext}.
 * The {@link org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext} defines the application details such as {@link org.apache.hadoop.yarn.api.records.ApplicationId}
 * and application name, the priority assigned to the application and the queue
 * to which this application needs to be assigned. In addition to this, the {@link org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext}
 * also defines the {@link org.apache.hadoop.yarn.api.records.ContainerLaunchContext} which describes the <code>Container</code> with which
 * the {@link com.wikibooks.hadoop.yarn.examples.MyApplicationMaster} is launched. </p>
 *
 * <p> The {@link org.apache.hadoop.yarn.api.records.ContainerLaunchContext} in this scenario defines the resources to be allocated for the
 * {@link com.wikibooks.hadoop.yarn.examples.MyApplicationMaster}'s container, the local resources (jars, configuration files) to be made available
 * and the environment to be set for the {@link com.wikibooks.hadoop.yarn.examples.MyApplicationMaster} and the commands to be executed to run the
 * {@link com.wikibooks.hadoop.yarn.examples.MyApplicationMaster}. <p>
 *
 * <p> Using the {@link org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext}, the client submits the application to the
 * <code>ResourceManager</code> and then monitors the application by requesting the <code>ResourceManager</code>
 * for an {@link org.apache.hadoop.yarn.api.records.ApplicationReport} at regular time intervals. In case of the application taking too long, the client
 * kills the application by submitting a {@link org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest} to the <code>ResourceManager</code>. </p>
 *
 */
public class MyClient {
  private static final Log LOG = LogFactory.getLog(MyClient.class);

  // Start time for client
  private final long clientStartTime = System.currentTimeMillis();

  // Configuration
  private Configuration conf;

  private YarnClient yarnClient;

  // Application master specific info to register a new Application with RM/ASM
  private String appName = "";

  // App master priority
  private int amPriority = 0;

  // Queue for App master
  private String amQueue = "";

  // Amt. of memory resource to request for to run the App Master
  private int amMemory = 10;

  // Amt. of virtual core resource to request for to run the App Master
  private int amVCores = 1;

  // ApplicationMaster jar file
  private String appMasterJarPath = "";

  // Container priority
  private int requestPriority = 0;

  // Amt of memory to request for container in which the HelloYarn will be executed
  private int containerMemory = 10;

  // Amt. of virtual cores to request for container in which the HelloYarn will be executed
  private int containerVirtualCores = 1;

  // No. of containers in which the HelloYarn needs to be executed
  private int numContainers = 1;

  // Timeout threshold for client. Kill app after time interval expires.
  private long clientTimeout = 600000;

  // Command line options
  private Options opts;

  /**
   * Constructor
   *
   */
  public MyClient() throws Exception  {
    createYarnClient();
    initOptions();
  }

  private void createYarnClient() {
    yarnClient = YarnClient.createYarnClient();
    this.conf = new YarnConfiguration();
    yarnClient.init(conf);
  }

  private void initOptions() {
    opts = new Options();
    opts.addOption("appname", true, "Application Name. Default value - HelloYarn");
    opts.addOption("priority", true, "Application Priority. Default 0");
    opts.addOption("queue", true, "RM Queue in which this application is to be submitted");
    opts.addOption("timeout", true, "Application timeout in milliseconds");
    opts.addOption("master_memory", true, "Amount of memory in MB to be requested to run the application master");
    opts.addOption("master_vcores", true, "Amount of virtual cores to be requested to run the application master");
    opts.addOption("jar", true, "Jar file containing the application master");
    opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run the HelloYarn");
    opts.addOption("container_vcores", true, "Amount of virtual cores to be requested to run the HelloYarn");
    opts.addOption("num_containers", true, "No. of containers on which the HelloYarn needs to be executed");
    opts.addOption("help", false, "Print usage");
  }


  /**
   * Helper function to print out usage
   */
  private void printUsage() {
    new HelpFormatter().printHelp("Client", opts);
  }

  /**
   * Parse command line options
   * @param args Parsed command line options
   * @return Whether the init was successful to run the client
   * @throws org.apache.commons.cli.ParseException
   */
  public boolean init(String[] args) throws ParseException {

    CommandLine cliParser = new GnuParser().parse(opts, args);

    if (args.length == 0) {
      throw new IllegalArgumentException("No args specified for client to initialize");
    }

    if (cliParser.hasOption("help")) {
      printUsage();
      return false;
    }

    appName = cliParser.getOptionValue("appname", "HelloYarn");
    amPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
    amQueue = cliParser.getOptionValue("queue", "default");
    amMemory = Integer.parseInt(cliParser.getOptionValue("master_memory", "10"));
    amVCores = Integer.parseInt(cliParser.getOptionValue("master_vcores", "1"));

    if (amMemory < 0) {
      throw new IllegalArgumentException("Invalid memory specified for application master, exiting."
          + " Specified memory=" + amMemory);
    }
    if (amVCores < 0) {
      throw new IllegalArgumentException("Invalid virtual cores specified for application master, exiting."
          + " Specified virtual cores=" + amVCores);
    }

    if (!cliParser.hasOption("jar")) {
      throw new IllegalArgumentException("No jar file specified for application master");
    }

    appMasterJarPath = cliParser.getOptionValue("jar");

    containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
    containerVirtualCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
    numContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));

    if (containerMemory < 0 || containerVirtualCores < 0 || numContainers < 1) {
      throw new IllegalArgumentException("Invalid no. of containers or container memory/vcores specified,"
          + " exiting."
          + " Specified containerMemory=" + containerMemory
          + ", containerVirtualCores=" + containerVirtualCores
          + ", numContainer=" + numContainers);
    }

    clientTimeout = Integer.parseInt(cliParser.getOptionValue("timeout", "600000"));

    return true;
  }

  /**
   * Main run function for the client
   * @return true if application completed successfully
   * @throws java.io.IOException
   * @throws org.apache.hadoop.yarn.exceptions.YarnException
   */
  public boolean run() throws IOException, YarnException {

    LOG.info("Running Client");
    yarnClient.start();

    // Get a new application id
    YarnClientApplication app = yarnClient.createApplication();
    GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

    int maxMem = appResponse.getMaximumResourceCapability().getMemory();
    LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

    // A resource ask cannot exceed the max.
    if (amMemory > maxMem) {
      LOG.info("AM memory specified above max threshold of cluster. Using max value."
          + ", specified=" + amMemory
          + ", max=" + maxMem);
      amMemory = maxMem;
    }

    int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
    LOG.info("Max virtual cores capabililty of resources in this cluster " + maxVCores);

    if (amVCores > maxVCores) {
      LOG.info("AM virtual cores specified above max threshold of cluster. "
          + "Using max value." + ", specified=" + amVCores
          + ", max=" + maxVCores);
      amVCores = maxVCores;
    }

    // set the application name
    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    ApplicationId appId = appContext.getApplicationId();

    appContext.setApplicationName(appName);

    // Set up resource type requirements
    // For now, both memory and vcores are supported, so we set memory and
    // vcores requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(amMemory);
    capability.setVirtualCores(amVCores);
    appContext.setResource(capability);

    // Set the priority for the application master
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(amPriority);
    appContext.setPriority(pri);

    // Set the queue to which this application is to be submitted in the RM
    appContext.setQueue(amQueue);

    // Set the ContainerLaunchContext to describe the Container ith which the ApplicationMaster is launched.
    appContext.setAMContainerSpec(getAMContainerSpec(appId.getId()));

    // Submit the application to the applications manager
    // SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);
    // Ignore the response as either a valid response object is returned on success
    // or an exception thrown to denote some form of a failure
    LOG.info("Submitting application to ASM");

    yarnClient.submitApplication(appContext);

    // Monitor the application
    return monitorApplication(appId);
  }

  private ContainerLaunchContext getAMContainerSpec(int appId) throws IOException, YarnException {
    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

    FileSystem fs = FileSystem.get(conf);

    // set local resources for the application master
    // local files or archives as needed
    // In this scenario, the jar file for the application master is part of the local resources
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

    LOG.info("Copy App Master jar from local filesystem and add to local environment");
    // Copy the application master jar to the filesystem
    // Create a local resource to point to the destination jar path
    addToLocalResources(fs, appMasterJarPath, Constants.AM_JAR_NAME, appId,
        localResources, null);

    // Set local resource info into app master container launch context
    amContainer.setLocalResources(localResources);

    // Set the env variables to be setup in the env where the application master will be run
    LOG.info("Set the environment for the application master");
    amContainer.setEnvironment(getAMEnvironment(localResources, fs));

    // Set the necessary command to execute the application master
    Vector<CharSequence> vargs = new Vector<CharSequence>(30);

    // Set java executable command
    LOG.info("Setting up app master command");
    vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
    // Set Xmx based on am memory size
    vargs.add("-Xmx" + amMemory + "m");
    // Set class name
    vargs.add("com.wikibooks.hadoop.yarn.examples.MyApplicationMaster");
    // Set params for Application Master
    vargs.add("--container_memory " + String.valueOf(containerMemory));
    vargs.add("--container_vcores " + String.valueOf(containerVirtualCores));
    vargs.add("--num_containers " + String.valueOf(numContainers));
    vargs.add("--priority " + String.valueOf(requestPriority));
    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

    // Get final command
    StringBuilder command = new StringBuilder();
    for (CharSequence str : vargs) {
      command.append(str).append(" ");
    }

    LOG.info("Completed setting up app master command " + command.toString());
    List<String> commands = new ArrayList<String>();
    commands.add(command.toString());
    amContainer.setCommands(commands);

    return amContainer;
  }

  private void addToLocalResources(FileSystem fs, String fileSrcPath,
                                   String fileDstPath, int appId, Map<String, LocalResource> localResources,
                                   String resources) throws IOException {
    String suffix = appName + "/" + appId + "/" + fileDstPath;
    Path dst =
        new Path(fs.getHomeDirectory(), suffix);
    if (fileSrcPath == null) {
      FSDataOutputStream ostream = null;
      try {
        ostream = FileSystem
            .create(fs, dst, new FsPermission((short) 0710));
        ostream.writeUTF(resources);
      } finally {
        IOUtils.closeQuietly(ostream);
      }
    } else {
      fs.copyFromLocalFile(new Path(fileSrcPath), dst);
    }
    FileStatus scFileStatus = fs.getFileStatus(dst);
    LocalResource scRsrc =
        LocalResource.newInstance(
            ConverterUtils.getYarnUrlFromURI(dst.toUri()),
            LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
            scFileStatus.getLen(), scFileStatus.getModificationTime());
    localResources.put(fileDstPath, scRsrc);
  }

  private Map<String, String> getAMEnvironment(Map<String, LocalResource> localResources
      , FileSystem fs) throws IOException{
    Map<String, String> env = new HashMap<String, String>();

    // Set ApplicationMaster jar file
    LocalResource appJarResource = localResources.get(Constants.AM_JAR_NAME);
    Path hdfsAppJarPath = new Path(fs.getHomeDirectory(), appJarResource.getResource().getFile());
    FileStatus hdfsAppJarStatus = fs.getFileStatus(hdfsAppJarPath);
    long hdfsAppJarLength = hdfsAppJarStatus.getLen();
    long hdfsAppJarTimestamp = hdfsAppJarStatus.getModificationTime();

    env.put(Constants.AM_JAR_PATH, hdfsAppJarPath.toString());
    env.put(Constants.AM_JAR_TIMESTAMP, Long.toString(hdfsAppJarTimestamp));
    env.put(Constants.AM_JAR_LENGTH, Long.toString(hdfsAppJarLength));

    // Add AppMaster.jar location to classpath
    // At some point we should not be required to add
    // the hadoop specific classpaths to the env.
    // It should be provided out of the box.
    // For now setting all required classpaths including
    // the classpath to "." for the application jar
    StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$())
        .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
    for (String c : conf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
      classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
      classPathEnv.append(c.trim());
    }
    env.put("CLASSPATH", classPathEnv.toString());

    return env;
  }

  /**
   * Monitor the submitted application for completion.
   * Kill application if time expires.
   * @param appId Application Id of application to be monitored
   * @return true if application completed successfully
   * @throws org.apache.hadoop.yarn.exceptions.YarnException
   * @throws java.io.IOException
   */
  private boolean monitorApplication(ApplicationId appId)
      throws YarnException, IOException {

    while (true) {
      // Check app status every 1 second.
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.error("Thread sleep in monitoring loop interrupted");
      }

      // Get application report for the appId we are interested in
      ApplicationReport report = yarnClient.getApplicationReport(appId);
      YarnApplicationState state = report.getYarnApplicationState();
      FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
      if (YarnApplicationState.FINISHED == state) {
        if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
          LOG.info("Application has completed successfully. "
              + " Breaking monitoring loop : ApplicationId:" + appId.getId());
          return true;
        }
        else {
          LOG.info("Application did finished unsuccessfully."
              + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
              + ". Breaking monitoring loop : ApplicationId:" + appId.getId());
          return false;
        }
      }
      else if (YarnApplicationState.KILLED == state
          || YarnApplicationState.FAILED == state) {
        LOG.info("Application did not finish."
            + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
            + ". Breaking monitoring loop : ApplicationId:" + appId.getId());
        return false;
      }

      if (System.currentTimeMillis() > (clientStartTime + clientTimeout)) {
        LOG.info("Reached client specified timeout for application. Killing application"
            + ". Breaking monitoring loop : ApplicationId:" + appId.getId());
        forceKillApplication(appId);
        return false;
      }
    }
  }

  /**
   * Kill a submitted application by sending a call to the ASM
   * @param appId Application Id to be killed.
   * @throws org.apache.hadoop.yarn.exceptions.YarnException
   * @throws java.io.IOException
   */
  private void forceKillApplication(ApplicationId appId)
      throws YarnException, IOException {
    yarnClient.killApplication(appId);

  }


  /**
   * @param args Command line arguments
   */
  public static void main(String[] args) {
    boolean result = false;
    try {
      MyClient client = new MyClient();
      LOG.info("Initializing Client");
      try {
        boolean doRun = client.init(args);
        if (!doRun) {
          System.exit(0);
        }
      } catch (IllegalArgumentException e) {
        System.err.println(e.getLocalizedMessage());
        client.printUsage();
        System.exit(-1);
      }
      result = client.run();
    } catch (Throwable t) {
      LOG.fatal("Error running CLient", t);
      System.exit(1);
    }
    if (result) {
      LOG.info("Application completed successfully");
      System.exit(0);
    }
    LOG.error("Application failed to complete successfully");
    System.exit(2);
  }
}
