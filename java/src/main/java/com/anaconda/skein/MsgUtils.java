package com.anaconda.skein;

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
        url.getHost(),
        url.getPort() == 0 ? -1 : url.getPort(),
        url.getFile());
  }

  public static Msg.Url writeUrl(URL url) {
    return Msg.Url.newBuilder()
        .setScheme(url.getScheme())
        .setHost(url.getHost())
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
        .setMaxRestarts(service.getMaxRestarts())
        .setResources(writeResources(service.getResources()))
        .putAllEnv(service.getEnv())
        .addAllCommands(service.getCommands())
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
        service.getMaxRestarts(),
        readResources(service.getResources()),
        localResources,
        new HashMap<String, String>(service.getEnvMap()),
        new ArrayList<String>(service.getCommandsList()),
        new HashSet<String>(service.getDependsList()));
  }

  public static Msg.ApplicationSpec writeApplicationSpec(Model.ApplicationSpec spec) {
    Msg.ApplicationSpec.Builder builder = Msg.ApplicationSpec.newBuilder()
        .setName(spec.getName())
        .setQueue(spec.getQueue())
        .setMaxAttempts(spec.getMaxAttempts())
        .addAllTags(spec.getTags());

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
    return new Model.ApplicationSpec(spec.getName(),
                                     spec.getQueue(),
                                     spec.getMaxAttempts(),
                                     new HashSet<String>(spec.getTagsList()),
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
    return out;
  }
}
