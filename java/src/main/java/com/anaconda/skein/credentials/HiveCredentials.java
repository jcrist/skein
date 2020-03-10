package com.anaconda.skein.credentials;

import com.anaconda.skein.Driver;
import com.anaconda.skein.Msg;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hive.jdbc.HiveConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

// Delegation token based connection is explained here:
// https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients#HiveServer2Clients-Multi-UserScenariosandProgrammaticLogintoKerberosKDC
// This class is inspired from org.apache.oozie.action.hadoop.Hive2Credentials which does the same thing for Oozie.
public class HiveCredentials implements CredentialProvider {
    private static final Logger LOG = LoggerFactory.getLogger(Driver.class);
    protected final String jdbcUrl;
    protected final String principal;
    protected final String user;

    public HiveCredentials(Msg.CredentialProviderSpec spec, String user) {
        this.jdbcUrl = spec.getUri();
        this.principal = spec.getPrincipal();
        this.user = user;
    }

    @Override
    public void updateCredentials(Credentials credentials) {
        try {
            // load the driver
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            String fullUrl = jdbcUrl + ";principal=" + principal + ";hive.server2.proxy.user=" + user;

            Connection con = null;
            String tokenStr = null;
            try {
                con = DriverManager.getConnection(fullUrl);
                LOG.info("Connected successfully to " + fullUrl);
                tokenStr = ((HiveConnection)con).getDelegationToken(user, principal);
            } finally {
                if (con != null) {
                    con.close();
                }
            }
            LOG.info("Got Hive Server token from " + fullUrl);

            Token<DelegationTokenIdentifier> hiveToken = new Token<DelegationTokenIdentifier>();
            hiveToken.decodeFromUrlString(tokenStr);
            credentials.addToken(getUniqueAlias(hiveToken), hiveToken);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public Text getUniqueAlias(Token<?> token) {
        return new Text(String.format("%s_%s_%d", token.getKind().toString(),
                token.getService().toString(), System.currentTimeMillis()));
    }
}
