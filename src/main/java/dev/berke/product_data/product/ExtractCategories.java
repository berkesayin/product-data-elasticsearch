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
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.RestClient;
import jakarta.annotation.PostConstruct;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import dev.berke.product_data.utils.Utils;

import javax.net.ssl.SSLContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// @ConditionalOnProperty(prefix = "extract.categories", name = "enabled", havingValue = "true", matchIfMissing = false)
@Component
public class ExtractCategories {

    private final Utils utils;

    public ExtractCategories(Utils utils) {
        this.utils = utils;
    }

    @PostConstruct
    public void extractCategories() throws Exception {

        // elasticsearch authentication
        var credentialsProvider = utils.createCredentialsProvider();

        // ssl context for certificates
        SSLContext sslContext = utils.getSSLContext();

        // map to store category counts
        Map<String, Integer> categoryCounts = new HashMap<>();

        try (RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200, "https"))
                .setRequestConfigCallback(configBuilder -> configBuilder
                        .setConnectTimeout(5000)
                        .setSocketTimeout(120000))
                .setHttpClientConfigCallback(builder -> builder
                        .setSSLContext(sslContext)
                        .setDefaultCredentialsProvider(credentialsProvider)
                        .setKeepAliveStrategy((response, context) -> 120000)
                        .setMaxConnTotal(100)
                        .setMaxConnPerRoute(100)
                        .setDefaultIOReactorConfig(IOReactorConfig.custom().setSoKeepAlive(true).build()))
                .build()) {

            RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
            ElasticsearchClient client = new ElasticsearchClient(transport);

            // scroll search on product index
            SearchRequest searchRequest = SearchRequest.of(s -> s
                    .index("product")
                    .scroll(Time.of(t -> t.time("50s")))
                    .query(q -> q.matchAll(mp -> mp)));
            SearchResponse<Map> searchResponse = client.search(searchRequest, Map.class);
            String scrollId = searchResponse.scrollId();
            List<Hit<Map>> hits = searchResponse.hits().hits();

            while (hits != null && !hits.isEmpty()) {
                for (Hit<Map> hit : hits) {
                    Map<String, Object> sourceMap = hit.source();

                    if (sourceMap != null && sourceMap.containsKey("category")) {
                        String category = sourceMap.get("category").toString();
                        categoryCounts.put(category, categoryCounts.getOrDefault(category, 0) + 1);
                    }
                }

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

            // index each category document with sequential id
            int idCounter = 1;
            for (Map.Entry<String, Integer> entry : categoryCounts.entrySet()) {
                final String currentId = String.valueOf(idCounter);
                Map<String, Object> categoryDoc = new HashMap<>();
                categoryDoc.put("category", entry.getKey());
                categoryDoc.put("category_id", currentId);
                categoryDoc.put("category_products", entry.getValue());
                client.index(i -> i
                        .index("category")
                        .id(currentId)
                        .document(categoryDoc));
                idCounter++;
            }
        }
    }
}
