package com.anaconda.crochet;

import org.eclipse.jetty.util.B64Code;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


public class AuthenticationFilter implements Filter {

    private byte[] secret = "be75fb6ad4b0468a8aea3c73618bca04".getBytes();

    private void unauthorized(HttpServletResponse resp)
            throws IOException {
        resp.setHeader("WWW-Authenticate", "crochet");
        resp.sendError(401, "Unauthorized");
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest servletRequest,
                         ServletResponse servletResponse,
                         FilterChain filterChain)
            throws IOException, ServletException {
        HttpServletRequest req = (HttpServletRequest) servletRequest;
        HttpServletResponse resp = (HttpServletResponse) servletResponse;

        String authHeader = req.getHeader("Authorization");
        String prefix = "crochet ";

        if (authHeader == null || !authHeader.toLowerCase().startsWith(prefix)) {
            unauthorized(resp);
            return;
        }
        String req_signature = authHeader.substring(prefix.length());

        Mac mac;
        MessageDigest md;
        try {
            mac = Mac.getInstance("HmacSHA1");
            mac.init(new SecretKeySpec(secret, "HmacSHA1"));
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException ex) {
            unauthorized(resp);
            return;
        } catch (InvalidKeyException ex) {
            unauthorized(resp);
            return;
        }

        mac.update(req.getMethod().getBytes(StandardCharsets.UTF_8));
        mac.update((byte)'\n');

        byte[] body = IOUtils.toByteArray(req.getInputStream());
        if (body.length > 0) {
            md.update(body);
            mac.update(md.digest());
        }
        mac.update((byte)'\n');

        String contentType = req.getContentType();
        if (contentType != null) {
            mac.update(contentType.getBytes(StandardCharsets.UTF_8));
        }
        mac.update((byte)'\n');

        String path = req.getRequestURI();
        if (path != null) {
            mac.update(path.getBytes(StandardCharsets.UTF_8));
        }

        String computed_signature = new String(B64Code.encode(mac.doFinal()));

        if (!req_signature.equals(computed_signature)) {
            unauthorized(resp);
            return;
        }

        filterChain.doFilter(new RequestWrapper(req, body), resp);
    }

    @Override
    public void destroy() {
    }
}
