package com.anaconda.skein.credentials;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.exceptions.YarnException;
import java.io.IOException;

public interface CredentialProvider {
    public void updateCredentials(Credentials credentials) throws IOException, YarnException;
}