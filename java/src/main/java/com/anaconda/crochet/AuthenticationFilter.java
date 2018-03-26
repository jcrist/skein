package com.anaconda.crochet;

import org.apache.commons.codec.binary.Base64;

import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.StringTokenizer;


public class AuthenticationFilter implements Filter {

    private String username = "testuser";

    private String password = "testpass";

    private String realm = "Protected";

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
            throws IOException, ServletException {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;

        String authHeader = request.getHeader("Authorization");

        if (authHeader == null) {
            unauthorized(response);
            return;
        }

        StringTokenizer st = new StringTokenizer(authHeader);
        if (st.hasMoreTokens()) {
            String basic = st.nextToken();

            if (basic.equalsIgnoreCase("Basic")) {
                try {
                    String credentials = new String(Base64.decodeBase64(st.nextToken()), "UTF-8");
                    int p = credentials.indexOf(":");

                    if (p == -1) {
                        unauthorized(response, "Invalid authentication token");
                        return;
                    }

                    String _username = credentials.substring(0, p).trim();
                    String _password = credentials.substring(p + 1).trim();

                    if (!username.equals(_username) || !password.equals(_password)) {
                        unauthorized(response, "Bad credentials");
                        return;
                    }

                    filterChain.doFilter(servletRequest, servletResponse);
                    return;
                }
                catch (UnsupportedEncodingException e) {
                    throw new Error("Couldn't retrieve authentication", e);
                }
            }
        }
    }

    @Override
    public void destroy() {
    }

    private void unauthorized(HttpServletResponse response, String message) throws IOException {
        response.setHeader("WWW-Authenticate", "Basic realm=\"" + realm + "\"");
        response.sendError(401, message);
    }

    private void unauthorized(HttpServletResponse response) throws IOException {
        unauthorized(response, "Unauthorized");
    }
}
