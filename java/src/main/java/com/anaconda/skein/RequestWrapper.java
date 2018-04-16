package com.anaconda.skein;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

public class RequestWrapper extends HttpServletRequestWrapper {
  private byte[] body;

  public RequestWrapper(HttpServletRequest request, byte[] body) {
    super(request);
    this.body = body;
  }

  @Override
  public ServletInputStream getInputStream() throws IOException {
    return new CachedServletInputStream(body);
  }

  @Override
  public BufferedReader getReader() throws IOException {
    return new BufferedReader(new InputStreamReader(getInputStream()));
  }

  public class CachedServletInputStream extends ServletInputStream {
    private ByteArrayInputStream input;

    public CachedServletInputStream(byte[] body) {
      input = new ByteArrayInputStream(body);
    }

    @Override
    public int read() throws IOException {
      return input.read();
    }

    @Override
    public boolean isFinished() {
      return false;
    }

    @Override
    public boolean isReady() {
      return true;
    }

    @Override
    public void setReadListener(ReadListener listener) {
      try {
        listener.onDataAvailable();
      } catch (IOException ex) {
        listener.onError(ex);
      }
    }
  }
}
