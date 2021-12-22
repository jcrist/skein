package com.anaconda.skein;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ System.class, Driver.class })
public class TestDriver {

  @Test
  public void testGetAppDirWithEnv() {
    FileSystem fs = Mockito.mock(FileSystem.class);
    ApplicationId appId = Mockito.mock(ApplicationId.class);
    PowerMockito.mockStatic(System.class);

    String stagingDir = "/skein_staging_dir_from_env";
    when(appId.toString()).thenReturn("application_01234");
    when(System.getenv("SKEIN_STAGING_DIR")).thenReturn(stagingDir);

    Driver driver = new Driver();
    Path actual = driver.getAppDir(fs, appId);
    Path expected = new Path(stagingDir, ".skein/application_01234");
    assertEquals(actual, expected);
  }

  @Test
  public void testGetAppDirWithNoEnv() {

    FileSystem fs = Mockito.mock(FileSystem.class);
    ApplicationId appId = Mockito.mock(ApplicationId.class);
    PowerMockito.mockStatic(System.class);

    String homeDir = "/myhomedir";
    when(fs.getHomeDirectory()).thenReturn(new Path(homeDir));
    when(appId.toString()).thenReturn("application_01234");
    when(System.getenv("SKEIN_STAGING_DIR")).thenReturn(null);

    Driver driver = new Driver();
    Path actual = driver.getAppDir(fs, appId);
    Path expected = new Path(homeDir, ".skein/application_01234");
    assertEquals(actual, expected);
  }
}
