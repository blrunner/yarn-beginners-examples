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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.LogManager;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MyApplicationMaster {
  private static final Log LOG = LogFactory.getLog(MyApplicationMaster.class);

  // Application Attempt Id ( combination of attemptId and fail count )
  protected ApplicationAttemptId appAttemptID;

  // No. of containers to run shell command on
  private int numTotalContainers = 1;

  // Memory to request for the container on which the shell command will run
  private int containerMemory = 10;

  // VirtualCores to request for the container on which the shell command will run
  private int containerVirtualCores = 1;

  // Priority of the request
  private int requestPriority;

  // Location of shell script ( obtained from info set in env )
  // Shell script path in fs
  private String appJarPath = "";
  // Timestamp needed for creating a local resource
  private long appJarTimestamp = 0;
  // File length needed for local resource
  private long appJarPathLen = 0;

  // Configuration
  private Configuration conf;

  public MyApplicationMaster() {
    // Set up the configuration
    conf = new YarnConfiguration();
  }

  /**
   * Parse command line options
   *
   * @param args Command line args
   * @return Whether init successful and run should be invoked
   * @throws org.apache.commons.cli.ParseException
   * @throws java.io.IOException
   */
  public boolean init(String[] args) throws Exception {
    Options opts = new Options();
    opts.addOption("app_attempt_id", true,
        "App Attempt ID. Not to be used unless for testing purposes");
    opts.addOption("shell_env", true,
        "Environment for shell script. Specified as env_key=env_val pairs");
    opts.addOption("container_memory", true,
        "Amount of memory in MB to be requested to run the shell command");
    opts.addOption("container_vcores", true,
        "Amount of virtual cores to be requested to run the shell command");
    opts.addOption("num_containers", true,
        "No. of containers on which the shell command needs to be executed");
    opts.addOption("priority", true, "Application Priority. Default 0");
    opts.addOption("help", false, "Print usage");

    CommandLine cliParser = new GnuParser().parse(opts, args);

    Map<String, String> envs = System.getenv();

    if (!envs.containsKey(ApplicationConstants.Environment.CONTAINER_ID.name())) {
      if (cliParser.hasOption("app_attempt_id")) {
        String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
        appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
      } else {
        throw new IllegalArgumentException(
            "Application Attempt Id not set in the environment");
      }
    } else {
      ContainerId containerId = ConverterUtils.toContainerId(envs
          .get(ApplicationConstants.Environment.CONTAINER_ID.name()));
      appAttemptID = containerId.getApplicationAttemptId();
    }

    if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
      throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV + " not set in the environment");
    }
    if (!envs.containsKey(ApplicationConstants.Environment.NM_HOST.name())) {
      throw new RuntimeException(ApplicationConstants.Environment.NM_HOST.name() + " not set in the environment");
    }
    if (!envs.containsKey(ApplicationConstants.Environment.NM_HTTP_PORT.name())) {
      throw new RuntimeException(ApplicationConstants.Environment.NM_HTTP_PORT + " not set in the environment");
    }
    if (!envs.containsKey(ApplicationConstants.Environment.NM_PORT.name())) {
      throw new RuntimeException(ApplicationConstants.Environment.NM_PORT.name() + " not set in the environment");
    }

    if (envs.containsKey(Constants.AM_JAR_PATH)) {
      appJarPath = envs.get(Constants.AM_JAR_PATH);

      if (envs.containsKey(Constants.AM_JAR_TIMESTAMP)) {
        appJarTimestamp = Long.valueOf(envs.get(Constants.AM_JAR_TIMESTAMP));
      }
      if (envs.containsKey(Constants.AM_JAR_LENGTH)) {
        appJarPathLen = Long.valueOf(envs.get(Constants.AM_JAR_LENGTH));
      }

      if (!appJarPath.isEmpty() && (appJarTimestamp <= 0 || appJarPathLen <= 0)) {
        LOG.error("Illegal values in env for shell script path" + ", path="
            + appJarPath + ", len=" + appJarPathLen + ", timestamp="+ appJarTimestamp);
        throw new IllegalArgumentException(
            "Illegal values in env for shell script path");
      }
    }

    LOG.info("Application master for app" + ", appId="
        + appAttemptID.getApplicationId().getId() + ", clusterTimestamp="
        + appAttemptID.getApplicationId().getClusterTimestamp()
        + ", attemptId=" + appAttemptID.getAttemptId());

    containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
    containerVirtualCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
    numTotalContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
    if (numTotalContainers == 0) {
      throw new IllegalArgumentException("Cannot run MyAppliCationMaster with no containers");
    }
    requestPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));

    return true;
  }

  /**
   * Main run function for the application master
   *
   * @throws org.apache.hadoop.yarn.exceptions.YarnException
   * @throws java.io.IOException
   */
  @SuppressWarnings({"unchecked"})
  public void run() throws Exception {
    LOG.info("Running MyApplicationMaster");

    // Initialize clients to ResourceManager and NodeManagers
    AMRMClient<ContainerRequest> amRMClient = AMRMClient.createAMRMClient();
    amRMClient.init(conf);
    amRMClient.start();

    // Register with ResourceManager
    amRMClient.registerApplicationMaster("", 0, "");

    // Set up resource type requirements for Container
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(containerMemory);
    capability.setVirtualCores(containerVirtualCores);

    // Priority for worker containers - priorities are intra-application
    Priority priority = Records.newRecord(Priority.class);
    priority.setPriority(requestPriority);

    // Make container requests to ResourceManager
    for (int i = 0; i < numTotalContainers; ++i) {
      ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
      amRMClient.addContainerRequest(containerAsk);
    }


    NMClient nmClient = NMClient.createNMClient();
    nmClient.init(conf);
    nmClient.start();

    // Setup CLASSPATH for Container
    Map<String, String> containerEnv = new HashMap<String, String>();
    containerEnv.put("CLASSPATH", "./*");

    // Setup ApplicationMaster jar file for Container
    LocalResource appMasterJar = createAppMasterJar();

    // Obtain allocated containers and launch
    int allocatedContainers = 0;
    // We need to start counting completed containers while still allocating
    // them since initial ones may complete while we're allocating subsequent
    // containers and if we miss those notifications, we'll never see them again
    // and this ApplicationMaster will hang indefinitely.
    int completedContainers = 0;
    while (allocatedContainers < numTotalContainers) {
      AllocateResponse response = amRMClient.allocate(0);
      for (Container container : response.getAllocatedContainers()) {
        allocatedContainers++;

        ContainerLaunchContext appContainer = createContainerLaunchContext(appMasterJar, containerEnv);
        LOG.info("Launching container " + allocatedContainers);

        nmClient.startContainer(container, appContainer);
      }
      for (ContainerStatus status : response.getCompletedContainersStatuses()) {
        ++completedContainers;
        LOG.info("ContainerID:" + status.getContainerId() + ", state:" + status.getState().name());
      }
      Thread.sleep(100);
    }

    // Now wait for the remaining containers to complete
    while (completedContainers < numTotalContainers) {
      AllocateResponse response = amRMClient.allocate(completedContainers / numTotalContainers);
      for (ContainerStatus status : response.getCompletedContainersStatuses()) {
        ++completedContainers;
        LOG.info("ContainerID:" + status.getContainerId() + ", state:" + status.getState().name());
      }
      Thread.sleep(100);
    }

    LOG.info("Completed containers:" + completedContainers);

    // Un-register with ResourceManager
    amRMClient.unregisterApplicationMaster(
        FinalApplicationStatus.SUCCEEDED, "", "");
    LOG.info("Finished MyApplicationMaster");
  }

  private LocalResource createAppMasterJar() throws IOException {
    LocalResource appMasterJar = Records.newRecord(LocalResource.class);
    if (!appJarPath.isEmpty()) {
      appMasterJar.setType(LocalResourceType.FILE);
      Path jarPath = new Path(appJarPath);
      jarPath = FileSystem.get(conf).makeQualified(jarPath);
       appMasterJar  .setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
      appMasterJar.setTimestamp(appJarTimestamp);
      appMasterJar.setSize(appJarPathLen);
      appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
    }
    return appMasterJar;
  }

  /**
   * Launch container by create ContainerLaunchContext
   *
   * @param appMasterJar
   * @param containerEnv
   * @return
   */
  private ContainerLaunchContext createContainerLaunchContext(LocalResource appMasterJar,
            Map<String, String> containerEnv) {
    ContainerLaunchContext appContainer =
        Records.newRecord(ContainerLaunchContext.class);
    appContainer.setLocalResources(
        Collections.singletonMap(Constants.AM_JAR_NAME, appMasterJar));
    appContainer.setEnvironment(containerEnv);
    appContainer.setCommands(
        Collections.singletonList(
            "$JAVA_HOME/bin/java" +
                " -Xmx256M" +
                " com.wikibooks.hadoop.yarn.examples.HelloYarn" +
                " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
        )
    );


    return appContainer;
  }

  public static void main(String[] args) throws Exception {
    try {
      MyApplicationMaster appMaster = new MyApplicationMaster();
      LOG.info("Initializing MyApplicationMaster");
      boolean doRun = appMaster.init(args);
      if (!doRun) {
        System.exit(0);
      }
      appMaster.run();
    } catch (Throwable t) {
      LOG.fatal("Error running MyApplicationMaster", t);
      LogManager.shutdown();
      ExitUtil.terminate(1, t);
    }
  }
}
