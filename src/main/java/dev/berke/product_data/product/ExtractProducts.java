package dev.berke.product_data.product;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.ScrollResponse;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

import javax.net.ssl.SSLContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dev.berke.product_data.utils.Utils;

// @ConditionalOnProperty(prefix = "extract.products", name = "enabled", havingValue = "true", matchIfMissing = false)
@Component
public class ExtractProducts {

    private final Utils utils;
    private static final Map<String, Integer> CATEGORY_ID_MAP;

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

    public ExtractProducts(Utils utils) {
        this.utils = utils;
    }

    @PostConstruct
    public void extractProducts() throws Exception {

        // elasticsearch authentication
        var credentialsProvider = utils.createCredentialsProvider();

        // ssl context for certificates
        SSLContext sslContext = utils.getSSLContext();

        try (RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200, "https"))
                .setRequestConfigCallback(configBuilder -> configBuilder.setConnectTimeout(5000)
                        .setSocketTimeout(120000))
                .setHttpClientConfigCallback(builder -> builder.setSSLContext(sslContext)
                        .setDefaultCredentialsProvider(credentialsProvider)
                        .setKeepAliveStrategy((response, context) -> 120000)
                        .setMaxConnTotal(100)
                        .setMaxConnPerRoute(100)
                        .setDefaultIOReactorConfig(
                                IOReactorConfig.custom().setSoKeepAlive(true).build()))
                .build()) {
            RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
            ElasticsearchClient client = new ElasticsearchClient(transport);

            // create a search request with scroll using the new client
            SearchRequest searchRequest = SearchRequest.of(s -> s
                    .index("kibana_sample_data_ecommerce")
                    .scroll(Time.of(t -> t.time("50s")))
                    .query(q -> q.matchAll(mp -> mp)));
            SearchResponse<Map> searchResponse = client.search(searchRequest, Map.class);
            String scrollId = searchResponse.scrollId();
            List<Hit<Map>> hits = searchResponse.hits().hits();

            while (hits != null && !hits.isEmpty()) {
                for (Hit<Map> hit : hits) {
                    Map<String, Object> sourceMap = hit.source();
                    if (sourceMap != null && sourceMap.containsKey("products")) {
                        List<Map> products = (List<Map>) sourceMap.get("products");
                        for (Map product : products) {
                            Map<String, Object> prodDoc = new HashMap<>();
                            prodDoc.put("base_price", product.get("base_price"));
                            prodDoc.put("manufacturer", product.get("manufacturer"));
                            prodDoc.put("product_id", product.get("product_id"));
                            prodDoc.put("category", product.get("category"));
                            prodDoc.put("sku", product.get("sku"));
                            prodDoc.put("min_price", product.get("min_price"));
                            prodDoc.put("created_on", product.get("created_on"));
                            prodDoc.put("product_name", product.get("product_name"));
                            prodDoc.put("status", 1);

                            String category = (String) product.get("category");
                            prodDoc.put("category_id", CATEGORY_ID_MAP.get(category));

                            client.index(i -> i
                                    .index("product")
                                    .id(product.get("product_id").toString())
                                    .document(prodDoc));
                        }
                    }
                }

                // scroll to next batch
                final String currentScrollId = scrollId;
                ScrollResponse<Map> scrollResponse = client.scroll(s -> s
                        .scrollId(currentScrollId)
                        .scroll(Time.of(t -> t.time("50s"))), Map.class);
                scrollId = scrollResponse.scrollId();
                hits = scrollResponse.hits().hits();
            }

            // clear scroll context
            final String finalScrollId = scrollId;
            client.clearScroll(c -> c.scrollId(finalScrollId));
        }
    }
}