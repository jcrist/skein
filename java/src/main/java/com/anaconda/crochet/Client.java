package com.anaconda.crochet;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Client {

  private static final Logger LOG = LogManager.getLogger(Client.class);

  private Configuration conf;
  private YarnClient yarnClient;
  private String amJar = "crochet-1.0-SNAPSHOT-jar-with-dependencies.jar";
  int amMemory = 10;
  int amVCores = 1;

  /** Main Entry Point. **/
  public static void main(String[] args) {
    boolean result = false;
    try {
      Client client = new Client();
      client.init();
      client.run();
    } catch (Throwable exc) {
      LOG.fatal("Error running Client", exc);
      System.exit(1);
    }
  }

  private void init() {
    conf = new YarnConfiguration();
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();
  }

  private void run() throws IOException, YarnException {
    YarnClientApplication app = yarnClient.createApplication();
    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    ApplicationId appId = appContext.getApplicationId();

    Map<String, LocalResource> localResources =
        new HashMap<String, LocalResource>();

    FileSystem fs = FileSystem.get(conf);
    addFile(localResources, fs, amJar, "crochet.jar", appId.toString());

    StringBuilder classPath = new StringBuilder(Environment.CLASSPATH.$$());
    classPath.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
    for (String c : conf.getStrings(
             YarnConfiguration.YARN_APPLICATION_CLASSPATH,
             YarnConfiguration
                 .DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
      classPath.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
      classPath.append(c.trim());
    }

    Map<String, String> env = new HashMap<String, String>();
    env.put("CLASSPATH", classPath.toString());
    env.put("CROCHET_SECRET_ACCESS_KEY", "foobar");

    String logdir = ApplicationConstants.LOG_DIR_EXPANSION_VAR;
    String command = (Environment.JAVA_HOME.$$() + "/bin/java "
                      + "-Xmx" + amMemory + "m "
                      + "com.anaconda.crochet.ApplicationMaster "
                      + "1>" + logdir + "/appmaster.stdout "
                      + "2>" + logdir + "/appmaster.stderr");


    List<String> commands = new ArrayList<String>();
    commands.add(command);

    ByteBuffer fsTokens = null;
    if (UserGroupInformation.isSecurityEnabled()) {
      Credentials credentials = new Credentials();
      String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
      if (tokenRenewer == null || tokenRenewer.length() == 0) {
        throw new IOException("Can't determine Yarn ResourceManager Kerberos "
                              + "principal for the RM to use as renewer");
      }

      fs.addDelegationTokens(tokenRenewer, credentials);
      DataOutputBuffer dob = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(dob);
      fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    }

    ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
        localResources, env, commands, null, fsTokens, null);

    appContext.setAMContainerSpec(amContainer);
    appContext.setApplicationName("crochet");
    appContext.setResource(Resource.newInstance(amMemory, amVCores));
    appContext.setPriority(Priority.newInstance(0));
    appContext.setQueue("default");

    LOG.info("Submitting application...");
    yarnClient.submitApplication(appContext);

    LOG.info("ApplicationID: " + appId);
  }

  private void addFile(Map<String, LocalResource> localResources,
                       FileSystem fs, String source, String target,
                       String appId) throws IOException {
    Path dest = new Path(fs.getHomeDirectory(),
                         ".crochet/" + appId + "/" + target);
    fs.copyFromLocalFile(new Path(source), dest);
    FileStatus status = fs.getFileStatus(dest);
    LocalResource resource = LocalResource.newInstance(
        ConverterUtils.getYarnUrlFromURI(dest.toUri()),
        LocalResourceType.FILE,
        LocalResourceVisibility.APPLICATION,
        status.getLen(),
        status.getModificationTime());
    localResources.put(target, resource);
  }
}
