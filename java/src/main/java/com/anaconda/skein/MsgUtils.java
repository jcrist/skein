package com.anaconda.skein;

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

import java.util.List;

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
}
