package dev.berke.product_data.utils;

import org.springframework.stereotype.Component;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.client.CredentialsProvider;
import org.springframework.beans.factory.annotation.Value;

import javax.net.ssl.SSLContext;
import java.security.cert.X509Certificate;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;

@Component
public class Utils {

    @Value("${elasticsearch.username}")
    private String username;

    @Value("${elasticsearch.password}")
    private String password;

    public static final Map<String, Integer> CATEGORY_ID_MAP;
    static {
        Map<String, Integer> map = new HashMap<>();
        map.put("Women's Accessories", 1);
        map.put("Women's Clothing", 2);
        map.put("Women's Shoes", 3);
        map.put("Men's Accessories", 4);
        map.put("Men's Shoes", 5);
        map.put("Men's Clothing", 6);
        CATEGORY_ID_MAP = Map.copyOf(map);
    }

    public CredentialsProvider createCredentialsProvider() {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        return credentialsProvider;
    }

    public static SSLContext getSSLContext() {
        try {
            TrustStrategy acceptingTrustStrategy = new TrustStrategy() {
                public boolean isTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                    return true;
                }
            };
            return SSLContexts.custom()
                    .loadTrustMaterial(null, acceptingTrustStrategy)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create SSL context", e);
        }
    }
}