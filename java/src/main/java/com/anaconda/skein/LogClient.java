package com.anaconda.skein;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogReader;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

public class LogClient {
  /* XXX: Unfortunately Hadoop doesn't have any nice public methods for
   * accessing logs. This class contains code pulled out of Hadoop 2.6.5, and
   * works with the *default* configuration of everything up to at least 3.0.2
   * (it may well work with later versions as well). The log handling was made
   * way more complicated in recent versions with
   * `LogAggregationFileController` and `LogAggregationFileControllerFactory`
   * java classes. These don't seem necessary to use when using the default
   * configuration, but our implementation here will likely break for other
   * configurations. I don't feel like using reflection yet, so punting on this
   * for now until someone has an issue.
   */
  public static class LogClientException extends IOException {
    public LogClientException(String msg) {
      super(msg);
    }
  }

  private static final String TMP_FILE_SUFFIX = ".tmp";

  private static Configuration conf;

  public LogClient(Configuration conf) {
    this.conf = conf;
  }

  public Path getRemoteAppLogDir(ApplicationId appId, String appOwner) {
    Path root = new Path(conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR)
    );
    Path out = new Path(root, appOwner);

    String suffix = conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX,
        YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR_SUFFIX);
    if (suffix != null && !suffix.isEmpty()) {
      out = new Path(out, suffix);
    }
    return new Path(out, appId.toString());
  }

  private static String rightTrimToString(byte[] buf) {
    // Remove trailing whitespace and convert to a string
    // Does so without an extra copy.
    int end = buf.length - 1;
    while ((end >= 0) && (buf[end] <= 32)) {
      end--;
    }
    return new String(buf, 0, end + 1);
  }

  public Map<String, String> getLogs(ApplicationId appId, String appOwner) throws IOException {
    Map<String, String> out = new HashMap<String, String>();

    Path remoteAppLogDir = getRemoteAppLogDir(appId, appOwner);
    RemoteIterator<FileStatus> nodeFiles;
    try {
      Path qualifiedLogDir =
          FileContext.getFileContext(conf).makeQualified(remoteAppLogDir);
      nodeFiles = FileContext.getFileContext(qualifiedLogDir.toUri(),
          conf).listStatus(remoteAppLogDir);
    } catch (FileNotFoundException fnf) {
      throw new LogClientException("Log aggregation has not completed or is not enabled.");
    }
    boolean logsFound = false;
    while (nodeFiles.hasNext()) {
      FileStatus thisNodeFile = nodeFiles.next();
      if (!thisNodeFile.getPath().getName().endsWith(TMP_FILE_SUFFIX)) {
        LogReader reader = new LogReader(conf, thisNodeFile.getPath());
        try {
          LogKey key = new LogKey();
          DataInputStream valueStream = reader.next(key);
          while (valueStream != null) {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            PrintStream printer = new PrintStream(os);
            while (true) {
              try {
                LogReader.readAContainerLogsForALogType(valueStream, printer,
                    thisNodeFile.getModificationTime());
                logsFound = true;
              } catch (EOFException eof) {
                break;
              }
            }
            out.put(key.toString(), rightTrimToString(os.toByteArray()));
            // Next container
            key = new LogKey();
            valueStream = reader.next(key);
          }
        } finally {
          reader.close();
        }
      }
    }
    if (!logsFound) {
      throw new LogClientException("No logs found. Log aggregation may have not "
                                    + "completed, or it may not be enabled.");
    }
    return out;
  }
}
