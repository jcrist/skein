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
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

public class LogClient {

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
    while (nodeFiles.hasNext()) {
      FileStatus thisNodeFile = nodeFiles.next();
      if (!thisNodeFile.getPath().getName().endsWith(TMP_FILE_SUFFIX)) {
        LogReader reader = new LogReader(conf, thisNodeFile.getPath());
        try {
          LogKey key = new LogKey();
          DataInputStream valueStream = reader.next(key);
          while (valueStream != null) {
            OutputStream os = new ByteArrayOutputStream();
            PrintStream printer = new PrintStream(os);
            while (true) {
              try {
                LogReader.readAContainerLogsForALogType(valueStream, printer,
                    thisNodeFile.getModificationTime());
              } catch (EOFException eof) {
                break;
              }
            }
            out.put(key.toString(), os.toString());
            // Next container
            key = new LogKey();
            valueStream = reader.next(key);
          }
        } finally {
          reader.close();
        }
      }
    }
    return out;
  }
}
