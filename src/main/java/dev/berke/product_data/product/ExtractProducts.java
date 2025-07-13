package dev.berke.product_data.product;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.ScrollResponse;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

import javax.net.ssl.SSLContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dev.berke.product_data.utils.Utils;

@ConditionalOnProperty(prefix = "extract.products", name = "enabled", havingValue = "true", matchIfMissing = false)
@Component
public class ExtractProducts {

    private static final Logger logger = LoggerFactory.getLogger(ExtractProducts.class);
    private final Utils utils;

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

            logger.info("Starting product extraction from kibana_sample_data_ecommerce...");

            // creating a search request with scroll
            SearchRequest searchRequest = SearchRequest.of(s -> s
                    .index("kibana_sample_data_ecommerce")
                    .scroll(Time.of(t -> t.time("1m")))
                    .size(1000)
                    .query(q -> q.matchAll(mp -> mp)));
            SearchResponse<Map> searchResponse = client.search(searchRequest, Map.class);
            String scrollId = searchResponse.scrollId();
            List<Hit<Map>> hits = searchResponse.hits().hits();

            // bulk processor for fast indexing
            List<Map<String, Object>> productsForBulk = new ArrayList<>();
            int totalProductsIndexed = 0;

            while (hits != null && !hits.isEmpty()) {
                for (Hit<Map> hit : hits) {
                    Map<String, Object> sourceMap = hit.source();
                    if (sourceMap != null && sourceMap.containsKey("products")) {
                        List<Map> products = (List<Map>) sourceMap.get("products");
                        for (Map product : products) {
                            Map<String, Object> prodDoc = new HashMap<>();

                            String categoryName = (String) product.get("category");
                            String productId = product.get("product_id").toString();

                            // nested object for the category
                            Map<String, Object> categoryObject = new HashMap<>();
                            categoryObject.put("id", Utils.CATEGORY_ID_MAP.get(categoryName).toString());
                            categoryObject.put("name", categoryName);

                            boolean status = true;

                            prodDoc.put("product_id", productId);
                            prodDoc.put("product_name", product.get("product_name"));
                            prodDoc.put("category", categoryObject);
                            prodDoc.put("base_price", product.get("base_price"));
                            prodDoc.put("min_price", product.get("min_price"));
                            prodDoc.put("manufacturer", product.get("manufacturer"));
                            prodDoc.put("sku", product.get("sku"));
                            prodDoc.put("created_on", product.get("created_on"));
                            prodDoc.put("status", status);

                            productsForBulk.add(prodDoc);
                        }
                    }
                }
                // indexing collected products in a single bulk request
                if (!productsForBulk.isEmpty()) {
                    bulkIndexProducts(client, productsForBulk);
                    totalProductsIndexed += productsForBulk.size();
                    logger.info("Indexed {} products. Total so far: {}", productsForBulk.size(), totalProductsIndexed);
                    productsForBulk.clear(); // clearing list for the next batch
                }

                // scrolling to the next batch
                final String currentScrollId = scrollId;
                ScrollResponse<Map> scrollResponse = client.scroll(s -> s
                        .scrollId(currentScrollId)
                        .scroll(Time.of(t -> t.time("1m"))), Map.class);
                scrollId = scrollResponse.scrollId();
                hits = scrollResponse.hits().hits();
            }

            final String finalScrollId = scrollId;
            if (finalScrollId != null) {
                client.clearScroll(c -> c.scrollId(finalScrollId));
            }
            logger.info("Product extraction finished. Total products indexed: {}", totalProductsIndexed);
        }
    }

    private void bulkIndexProducts(
            ElasticsearchClient client,
            List<Map<String, Object>> products
    ) throws Exception {
        BulkRequest.Builder br = new BulkRequest.Builder();

        for (Map<String, Object> product : products) {
            br.operations(op -> op
                    .index(idx -> idx
                            .index("product")
                            .id(product.get("product_id").toString())
                            .document(product)
                    )
            );
        }

        BulkResponse result = client.bulk(br.build());

        if (result.errors()) {
            logger.error("Bulk had errors");
            for (BulkResponseItem item : result.items()) {
                if (item.error() != null) {
                    logger.error("Error for ID {}: {}", item.id(), item.error().reason());
                }
            }
        }
    }
}