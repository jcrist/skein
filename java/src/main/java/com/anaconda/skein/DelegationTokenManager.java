package com.anaconda.skein;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hive.jdbc.HiveConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DelegationTokenManager {
    private static final Logger LOG = LoggerFactory.getLogger(DelegationTokenManager.class);
    private Credentials credentials;
    private final List<Model.DelegationTokenProvider> delegationTokenProviders;

    public DelegationTokenManager() { this.delegationTokenProviders=new LinkedList<>(); }

    public void addTokenProvider(Model.DelegationTokenProvider p) { this.delegationTokenProviders.add(p); }

    public void initializeCredentials(Credentials credentials) { this.credentials = credentials; }

    public void obtainTokensHDFS(YarnClient yarnClient, FileSystem fs, Model.ApplicationSpec spec,
                                      Configuration conf) throws IOException, YarnException {
        // Collect security tokens as needed
        LOG.debug("Collecting filesystem delegation tokens");

        List<Path> l= spec.getFileSystems();
        l.add(0, new Path(fs.getUri()));

        TokenCache.obtainTokensForNamenodes(this.credentials,
                l.toArray(new Path[l.size()]), conf);

        boolean hasRMToken = false;
        for (Token<?> token: this.credentials.getAllTokens()) {
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
            this.credentials.addToken(rmDelegationTokenService, rmToken);
        }
    }

    private Text getUniqueAlias(Token<?> token) {
        return new Text(String.format("%s_%s_%d", token.getKind().toString(),
                token.getService().toString(), System.currentTimeMillis()));
    }

    // Delegation token based connection is explained here:
    // https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients#HiveServer2Clients-Multi-UserScenariosandProgrammaticLogintoKerberosKDC
    // This method is inspired from org.apache.oozie.action.hadoop.Hive2Credentials which does the same thing for Oozie.
    public void obtainTokensHive(Map<String, String> config, String user) {
        String jdbcUrl = config.get("hive.jdbc.url");
        String principal = config.get("hive.jdbc.principal");
        String fullUrl = jdbcUrl + ";principal=" + principal + ";hive.server2.proxy.user=" + user;

        try {
            // load the driver
            Class.forName("org.apache.hive.jdbc.HiveDriver");

            Connection con = null;
            String tokenStr = null;
            try {
                con = DriverManager.getConnection(fullUrl);
                LOG.info("Connected successfully to " + fullUrl);
                tokenStr = ((HiveConnection)con).getDelegationToken(user, principal);
            } finally {
                if (con != null) { con.close(); }
            }
            LOG.info("Got Hive Server token from " + fullUrl);

            Token<DelegationTokenIdentifier> hiveToken = new Token<DelegationTokenIdentifier>();
            hiveToken.decodeFromUrlString(tokenStr);
            credentials.addToken(getUniqueAlias(hiveToken), hiveToken);
        } catch (IOException | SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    // For some systems (like Hive) to obtain the delegation token we need to do it
    // while we are authenticated with kerberos, before we impersonate the user.
    public void obtainTokensWithoutImpersonation(String userWeAuthenticateFor) throws IllegalArgumentException {
        for (Model.DelegationTokenProvider p : this.delegationTokenProviders) {
            if(p.getName().equals("hive")) {
                this.obtainTokensHive(p.getConfig(), userWeAuthenticateFor);
            } else {
                throw new IllegalArgumentException("The Provider for Delegation Token was not found");
            }
        }
    }

    public ByteBuffer toBytes() throws IOException {
        DataOutputBuffer dob = new DataOutputBuffer();
        this.credentials.writeTokenStorageToStream(dob);
        return ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    }
}
