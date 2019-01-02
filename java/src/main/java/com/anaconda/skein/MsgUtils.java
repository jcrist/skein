package com.anaconda.skein;

import com.google.common.base.Functions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.log4j.Level;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class MsgUtils {
  public static final Msg.Empty EMPTY = Msg.Empty.newBuilder().build();

  public static Msg.ApplicationState.Type writeApplicationState(YarnApplicationState state) {
    switch (state) {
      case NEW:
        return Msg.ApplicationState.Type.NEW;
      case NEW_SAVING:
        return Msg.ApplicationState.Type.NEW_SAVING;
      case SUBMITTED:
        return Msg.ApplicationState.Type.SUBMITTED;
      case ACCEPTED:
        return Msg.ApplicationState.Type.ACCEPTED;
      case RUNNING:
        return Msg.ApplicationState.Type.RUNNING;
      case FINISHED:
        return Msg.ApplicationState.Type.FINISHED;
      case FAILED:
        return Msg.ApplicationState.Type.FAILED;
      case KILLED:
        return Msg.ApplicationState.Type.KILLED;
    }
    return null; // appease the compiler, but can't get here
  }

  public static YarnApplicationState readApplicationState(Msg.ApplicationState.Type state) {
    switch (state) {
      case NEW:
        return YarnApplicationState.NEW;
      case NEW_SAVING:
        return YarnApplicationState.NEW_SAVING;
      case SUBMITTED:
        return YarnApplicationState.SUBMITTED;
      case ACCEPTED:
        return YarnApplicationState.ACCEPTED;
      case RUNNING:
        return YarnApplicationState.RUNNING;
      case FINISHED:
        return YarnApplicationState.FINISHED;
      case FAILED:
        return YarnApplicationState.FAILED;
      case KILLED:
        return YarnApplicationState.KILLED;
    }
    return null; // appease the compiler, but can't get here
  }

  public static Msg.FinalStatus.Type writeFinalStatus(FinalApplicationStatus status) {
    switch (status) {
      case UNDEFINED:
        return Msg.FinalStatus.Type.UNDEFINED;
      case SUCCEEDED:
        return Msg.FinalStatus.Type.SUCCEEDED;
      case FAILED:
        return Msg.FinalStatus.Type.FAILED;
      case KILLED:
        return Msg.FinalStatus.Type.KILLED;
    }
    return null; // appease the compiler, but can't get here
  }

  public static FinalApplicationStatus readFinalStatus(Msg.FinalStatus.Type status) {
    switch (status) {
      case UNDEFINED:
        return FinalApplicationStatus.UNDEFINED;
      case SUCCEEDED:
        return FinalApplicationStatus.SUCCEEDED;
      case FAILED:
        return FinalApplicationStatus.FAILED;
      case KILLED:
        return FinalApplicationStatus.KILLED;
    }
    return null; // appease the compiler, but can't get here
  }

  public static Msg.File.Type writeFileType(LocalResourceType typ) {
    switch (typ) {
      case FILE:
        return Msg.File.Type.FILE;
      case ARCHIVE:
        return Msg.File.Type.ARCHIVE;
      case PATTERN:
        throw new IllegalArgumentException("Invalid resource type: " + typ);
    }
    return null; // appease the compiler, but can't get here
  }

  public static LocalResourceType readFileType(Msg.File.Type typ) {
    switch (typ) {
      case FILE:
        return LocalResourceType.FILE;
      case ARCHIVE:
        return LocalResourceType.ARCHIVE;
    }
    return null; // appease the compiler, but can't get here
  }

  public static Msg.File.Visibility writeFileVisibility(LocalResourceVisibility vis) {
    switch (vis) {
      case PUBLIC:
        return Msg.File.Visibility.PUBLIC;
      case PRIVATE:
        return Msg.File.Visibility.PRIVATE;
      case APPLICATION:
        return Msg.File.Visibility.APPLICATION;
    }
    return null; // appease the compiler, but can't get here
  }

  public static LocalResourceVisibility readFileVisibility(Msg.File.Visibility vis) {
    switch (vis) {
      case PUBLIC:
        return LocalResourceVisibility.PUBLIC;
      case PRIVATE:
        return LocalResourceVisibility.PRIVATE;
      case APPLICATION:
        return LocalResourceVisibility.APPLICATION;
    }
    return null; // appease the compiler, but can't get here
  }

  public static Msg.Container.State writeContainerState(Model.Container.State state) {
    switch (state) {
      case WAITING:
        return Msg.Container.State.WAITING;
      case REQUESTED:
        return Msg.Container.State.REQUESTED;
      case RUNNING:
        return Msg.Container.State.RUNNING;
      case SUCCEEDED:
        return Msg.Container.State.SUCCEEDED;
      case FAILED:
        return Msg.Container.State.FAILED;
      case KILLED:
        return Msg.Container.State.KILLED;
    }
    return null; // appease the compiler, but can't get here
  }

  public static Model.Container.State readContainerState(Msg.Container.State state) {
    switch (state) {
      case WAITING:
        return Model.Container.State.WAITING;
      case REQUESTED:
        return Model.Container.State.REQUESTED;
      case RUNNING:
        return Model.Container.State.RUNNING;
      case SUCCEEDED:
        return Model.Container.State.SUCCEEDED;
      case FAILED:
        return Model.Container.State.FAILED;
      case KILLED:
        return Model.Container.State.KILLED;
    }
    return null; // appease the compiler, but can't get here
  }

  public static Msg.ApplicationReport writeApplicationReport(
      ApplicationReport r) {
    return Msg.ApplicationReport.newBuilder()
        .setId(r.getApplicationId().toString())
        .setName(r.getName())
        .setUser(r.getUser())
        .setQueue(r.getQueue())
        .setHost(r.getHost())
        .setPort(r.getRpcPort())
        .setTrackingUrl(r.getTrackingUrl())
        .setState(writeApplicationState(r.getYarnApplicationState()))
        .setFinalStatus(writeFinalStatus(r.getFinalApplicationStatus()))
        .setProgress(r.getProgress())
        .setDiagnostics(r.getDiagnostics())
        .setStartTime(r.getStartTime())
        .setFinishTime(r.getFinishTime())
        .addAllTags(r.getApplicationTags())
        .setUsage(writeUsageReport(r.getApplicationResourceUsageReport()))
        .build();
  }

  public static Msg.ResourceUsageReport writeUsageReport(
      ApplicationResourceUsageReport r) {
    return Msg.ResourceUsageReport.newBuilder()
      .setMemorySeconds(Math.max(0, r.getMemorySeconds()))
      .setVcoreSeconds(Math.max(0, r.getVcoreSeconds()))
      .setNumUsedContainers(Math.max(0, r.getNumUsedContainers()))
      .setReservedResources(writeResources(r.getReservedResources()))
      .setNeededResources(writeResources(r.getNeededResources()))
      .setUsedResources(writeResources(r.getUsedResources()))
      .build();
  }

  public static Resource readResources(Msg.Resources r) {
    return Resource.newInstance(r.getMemory(), r.getVcores());
  }

  public static Msg.Resources writeResources(Resource r) {
    return Msg.Resources.newBuilder()
      .setMemory(Math.max(0, r.getMemory()))
      .setVcores(Math.max(0, r.getVirtualCores()))
      .build();
  }

  public static Msg.ApplicationsResponse writeApplicationsResponse(
      List<ApplicationReport> reports) {
    Msg.ApplicationsResponse.Builder builder = Msg.ApplicationsResponse.newBuilder();
    for (ApplicationReport report : reports) {
      builder.addReports(writeApplicationReport(report));
    }
    return builder.build();
  }

  public static URL readUrl(Msg.Url url) {
    return URL.newInstance(
        url.getScheme(),
        Strings.emptyToNull(url.getHost()),
        url.getPort() == 0 ? -1 : url.getPort(),
        url.getFile());
  }

  public static Msg.Url writeUrl(URL url) {
    return Msg.Url.newBuilder()
        .setScheme(url.getScheme())
        .setHost(Strings.nullToEmpty(url.getHost()))
        .setPort(url.getPort())
        .setFile(url.getFile())
        .build();
  }

  public static Msg.File writeFile(LocalResource r) {
    return Msg.File.newBuilder()
        .setSource(writeUrl(r.getResource()))
        .setType(writeFileType(r.getType()))
        .setVisibility(writeFileVisibility(r.getVisibility()))
        .setSize(r.getSize())
        .setTimestamp(r.getTimestamp())
        .build();
  }

  public static LocalResource readFile(Msg.File f) {
    return LocalResource.newInstance(
        readUrl(f.getSource()),
        readFileType(f.getType()),
        readFileVisibility(f.getVisibility()),
        f.getSize(),
        f.getTimestamp());
  }

  public static Msg.Service writeService(Model.Service service) {
    Msg.Service.Builder builder = Msg.Service.newBuilder()
        .setInstances(service.getInstances())
        .setNodeLabel(service.getNodeLabel())
        .addAllNodes(service.getNodes())
        .addAllRacks(service.getRacks())
        .setRelaxLocality(service.getRelaxLocality())
        .setMaxRestarts(service.getMaxRestarts())
        .setResources(writeResources(service.getResources()))
        .putAllEnv(service.getEnv())
        .setScript(service.getScript())
        .addAllDepends(service.getDepends());

    for (Map.Entry<String, LocalResource> entry : service.getLocalResources().entrySet()) {
      builder.putFiles(entry.getKey(), writeFile(entry.getValue()));
    }
    return builder.build();
  }

  public static Model.Service readService(Msg.Service service) {
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    for (Map.Entry<String, Msg.File> entry : service.getFilesMap().entrySet()) {
      localResources.put(entry.getKey(), readFile(entry.getValue()));
    }
    return new Model.Service(
        service.getInstances(),
        service.getNodeLabel(),
        new ArrayList<String>(service.getNodesList()),
        new ArrayList<String>(service.getRacksList()),
        service.getRelaxLocality(),
        service.getMaxRestarts(),
        readResources(service.getResources()),
        localResources,
        new HashMap<String, String>(service.getEnvMap()),
        service.getScript(),
        new HashSet<String>(service.getDependsList()));
  }

  public static Msg.Acls writeAcls(Model.Acls acl) {
    return Msg.Acls.newBuilder()
              .setEnable(acl.getEnable())
              .addAllViewUsers(acl.getViewUsers())
              .addAllViewGroups(acl.getViewGroups())
              .addAllModifyUsers(acl.getModifyUsers())
              .addAllModifyGroups(acl.getModifyGroups())
              .addAllUiUsers(acl.getUiUsers())
              .build();
  }

  public static Model.Acls readAcls(Msg.Acls acl) {
    return new Model.Acls(
        acl.getEnable(),
        acl.getViewUsersList(),
        acl.getViewGroupsList(),
        acl.getModifyUsersList(),
        acl.getModifyGroupsList(),
        acl.getUiUsersList()
    );
  }

  public static Msg.Log.Level writeLogLevel(Level logLevel) {
    switch (logLevel.toInt()) {
      case Level.INFO_INT:
        return Msg.Log.Level.INFO;
      case Level.ALL_INT:
        return Msg.Log.Level.ALL;
      case Level.TRACE_INT:
        return Msg.Log.Level.TRACE;
      case Level.DEBUG_INT:
        return Msg.Log.Level.DEBUG;
      case Level.WARN_INT:
        return Msg.Log.Level.WARN;
      case Level.ERROR_INT:
        return Msg.Log.Level.ERROR;
      case Level.FATAL_INT:
        return Msg.Log.Level.FATAL;
      case Level.OFF_INT:
        return Msg.Log.Level.OFF;
    }
    return null; // appease the compiler, but can't get here
  }

  public static Level readLogLevel(Msg.Log.Level logLevel) {
    switch (logLevel) {
      case INFO:
        return Level.INFO;
      case ALL:
        return Level.ALL;
      case TRACE:
        return Level.TRACE;
      case DEBUG:
        return Level.DEBUG;
      case WARN:
        return Level.WARN;
      case ERROR:
        return Level.ERROR;
      case FATAL:
        return Level.FATAL;
      case OFF:
        return Level.OFF;
    }
    return null; // appease the compiler, but can't get here
  }

  public static Msg.Security writeSecurity(Model.Security security) {
    Msg.Security.Builder builder = Msg.Security.newBuilder();

    if (security.getCertBytes() != null) {
      builder.setCertBytes(security.getCertBytes());
    } else if (security.getCertFile() != null) {
      builder.setCertFile(writeFile(security.getCertFile()));
    }

    if (security.getKeyBytes() != null) {
      builder.setKeyBytes(security.getKeyBytes());
    } else if (security.getKeyFile() != null) {
      builder.setKeyFile(writeFile(security.getKeyFile()));
    }

    return builder.build();
  }

  public static Model.Security readSecurity(Msg.Security security) {
    Model.Security result = new Model.Security();

    switch (security.getCertCase()) {
      case CERT_BYTES:
        result.setCertBytes(security.getCertBytes());
        break;
      case CERT_FILE:
        result.setCertFile(readFile(security.getCertFile()));
        break;
      case CERT_NOT_SET:
        break;
    }

    switch (security.getKeyCase()) {
      case KEY_BYTES:
        result.setKeyBytes(security.getKeyBytes());
        break;
      case KEY_FILE:
        result.setKeyFile(readFile(security.getKeyFile()));
        break;
      case KEY_NOT_SET:
        break;
    }
    return result;
  }

  public static Msg.Master writeMaster(Model.Master master) {
    Msg.Master.Builder builder = Msg.Master.newBuilder()
        .setResources(writeResources(master.getResources()))
        .putAllEnv(master.getEnv())
        .setScript(master.getScript())
        .setLogLevel(writeLogLevel(master.getLogLevel()));

    for (Map.Entry<String, LocalResource> entry : master.getLocalResources().entrySet()) {
      builder.putFiles(entry.getKey(), writeFile(entry.getValue()));
    }

    if (master.hasSecurity()) {
      builder.setSecurity(writeSecurity(master.getSecurity()));
    }

    if (master.hasLogConfig()) {
      builder.setLogConfig(writeFile(master.getLogConfig()));
    }

    return builder.build();
  }

  public static Model.Master readMaster(Msg.Master master) {
    Model.Master out = new Model.Master();

    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    for (Map.Entry<String, Msg.File> entry : master.getFilesMap().entrySet()) {
      localResources.put(entry.getKey(), readFile(entry.getValue()));
    }
    out.setLocalResources(localResources);
    out.setResources(readResources(master.getResources()));
    out.setEnv(new HashMap<String, String>(master.getEnvMap()));
    out.setScript(master.getScript());

    if (master.hasLogConfig()) {
      out.setLogConfig(readFile(master.getLogConfig()));
    }
    if (master.hasSecurity()) {
      out.setSecurity(readSecurity(master.getSecurity()));
    }
    out.setLogLevel(readLogLevel(master.getLogLevel()));
    return out;
  }

  public static Msg.ApplicationSpec writeApplicationSpec(Model.ApplicationSpec spec) {
    Msg.ApplicationSpec.Builder builder = Msg.ApplicationSpec.newBuilder()
        .setName(spec.getName())
        .setQueue(spec.getQueue())
        .setUser(spec.getUser())
        .setNodeLabel(spec.getNodeLabel())
        .setMaxAttempts(spec.getMaxAttempts())
        .addAllTags(spec.getTags())
        .setAcls(writeAcls(spec.getAcls()))
        .setMaster(writeMaster(spec.getMaster()))
        .addAllFileSystems(Lists.transform(spec.getFileSystems(), Functions.toStringFunction()));

    for (Map.Entry<String, Model.Service> entry : spec.getServices().entrySet()) {
      builder.putServices(entry.getKey(), writeService(entry.getValue()));
    }
    return builder.build();
  }

  public static Model.ApplicationSpec readApplicationSpec(Msg.ApplicationSpec spec) {
    Map<String, Model.Service> services = new HashMap<String, Model.Service>();
    for (Map.Entry<String, Msg.Service> entry : spec.getServicesMap().entrySet()) {
      services.put(entry.getKey(), readService(entry.getValue()));
    }

    final List<Path> fileSystems = new ArrayList<Path>();
    for (int i = 0; i < spec.getFileSystemsCount(); i++) {
      fileSystems.add(new Path(spec.getFileSystems(i)));
    }

    return new Model.ApplicationSpec(spec.getName(),
                                     spec.getQueue(),
                                     spec.getUser(),
                                     spec.getNodeLabel(),
                                     spec.getMaxAttempts(),
                                     new HashSet<String>(spec.getTagsList()),
                                     fileSystems,
                                     readAcls(spec.getAcls()),
                                     readMaster(spec.getMaster()),
                                     services);
  }

  public static Msg.Container writeContainer(Model.Container container) {

    Msg.Container.Builder builder = Msg.Container.newBuilder()
        .setServiceName(container.getServiceName())
        .setInstance(container.getInstance())
        .setState(writeContainerState(container.getState()))
        .setStartTime(container.getStartTime())
        .setFinishTime(container.getFinishTime());

    ContainerId containerId = container.getYarnContainerId();
    if (containerId != null) {
      builder.setYarnContainerId(containerId.toString());
    }
    String nodeHttpAddress = container.getYarnNodeHttpAddress();
    if (nodeHttpAddress != null) {
      builder.setYarnNodeHttpAddress(nodeHttpAddress);
    }
    String exitMessage = container.getExitMessage();
    if (exitMessage != null) {
      builder.setExitMessage(exitMessage);
    }
    return builder.build();
  }

  public static Model.Container readContainer(Msg.Container container) {
    Model.Container out = new Model.Container();
    out.setServiceName(container.getServiceName());
    out.setInstance(container.getInstance());
    out.setState(readContainerState(container.getState()));
    out.setYarnContainerId(ContainerId.fromString(container.getYarnContainerId()));
    out.setStartTime(container.getStartTime());
    out.setFinishTime(container.getFinishTime());
    out.setExitMessage(container.getExitMessage());
    return out;
  }
}
