package com.anaconda.skein.credentials;

import com.anaconda.skein.Driver;
import com.anaconda.skein.Model;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class HdfsCredentials implements CredentialProvider {
    private static final Logger LOG = LoggerFactory.getLogger(Driver.class);
    private final YarnClient yarnClient;
    private final FileSystem fs;
    private final Model.ApplicationSpec spec;
    private final Configuration conf;

    public HdfsCredentials(YarnClient yarnClient, FileSystem fs,
                           Model.ApplicationSpec spec, Configuration conf) {
        this.yarnClient = yarnClient;
        this.fs = fs;
        this.spec = spec;
        this.conf=conf;
    }

    @Override
    public void updateCredentials(Credentials credentials) throws IOException, YarnException {
        // Collect security tokens as needed
        LOG.debug("Collecting filesystem delegation tokens");

        List<Path> l= spec.getFileSystems();
        l.add(0, new Path(fs.getUri()));

        TokenCache.obtainTokensForNamenodes(credentials,
                l.toArray(new Path[l.size()]), conf);

        boolean hasRMToken = false;
        for (Token<?> token: credentials.getAllTokens()) {
            if (token.getKind().equals(RMDelegationTokenIdentifier.KIND_NAME)) {
                LOG.debug("RM delegation token already acquired");
                hasRMToken = true;
                break;
            }
        }
        if (!hasRMToken) {
            LOG.debug("Adding RM delegation token");
            Text rmDelegationTokenService = ClientRMProxy.getRMDelegationTokenService(conf);
            String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
            org.apache.hadoop.yarn.api.records.Token rmDelegationToken =
                    yarnClient.getRMDelegationToken(new Text(tokenRenewer));
            Token<TokenIdentifier> rmToken = ConverterUtils.convertFromYarn(
                    rmDelegationToken, rmDelegationTokenService
            );
            credentials.addToken(rmDelegationTokenService, rmToken);
        }
    }
}
