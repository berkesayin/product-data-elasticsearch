package dev.berke.product_data.product;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.core.ClearScrollRequest;
import co.elastic.clients.elasticsearch.core.ScrollRequest;
import co.elastic.clients.elasticsearch.core.ScrollResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import dev.berke.product_data.utils.Utils;
import jakarta.annotation.PostConstruct;
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@ConditionalOnProperty(prefix = "extract.categories", name = "enabled", havingValue = "true", matchIfMissing = false)
@Component
public class ExtractCategories {

    private final Utils utils;
    private static final Logger log = LoggerFactory.getLogger(ExtractCategories.class);

    public ExtractCategories(Utils utils) {
        this.utils = utils;
    }

    @PostConstruct
    public void extractCategories() {
        log.info("Starting category extraction...");

        RestClient restClient = null;
        String scrollId = null;

        try {
            // elasticsearch authentication
            var credentialsProvider = utils.createCredentialsProvider();

            // SSL context for certificates
            SSLContext sslContext = utils.getSSLContext();

            restClient = RestClient.builder(new HttpHost("localhost", 9200, "https"))
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
                    .build();

            RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
            ElasticsearchClient client = new ElasticsearchClient(transport);

            // store unique category names efficiently
            Set<String> uniqueCategoryNames = new HashSet<>();
            log.info("Scrolling through 'product' index to find unique categories...");
            SearchRequest searchRequest = SearchRequest.of(s -> s
                    .index("product") // Target index
                    .scroll(Time.of(t -> t.time("1m")))
                    .size(1000)
                    .source(src -> src
                            .filter(f -> f
                                    .includes("category_name") // only fetch this field
                            )
                    )
                    .query(q -> q
                            .matchAll(m -> m))
            );

            SearchResponse<Map> searchResponse = client.search(searchRequest, Map.class);
            scrollId = searchResponse.scrollId();
            List<Hit<Map>> hits = searchResponse.hits().hits();
            long totalProductsScanned = 0;

            // scroll loop
            while (hits != null && !hits.isEmpty()) {
                totalProductsScanned += hits.size();
                log.debug("Processing batch of {} products... (Total scanned: {})", hits.size(), totalProductsScanned);
                for (Hit<Map> hit : hits) {
                    Map<String, Object> sourceMap = hit.source();

                    if (sourceMap != null && sourceMap.containsKey("category_name")) {
                        Object categoryNameObj = sourceMap.get("category_name");
                        if (categoryNameObj != null) {
                            String categoryName = categoryNameObj.toString();
                            uniqueCategoryNames.add(categoryName);
                        } else {
                            log.warn("Product document {} has null category_name field", hit.id());
                        }
                    } else {
                        log.warn("Product document {} missing category_name field", hit.id());
                    }
                }

                if (scrollId == null) {
                    log.warn("Scroll ID became null unexpectedly.");
                    break;
                }
                final String currentScrollId = scrollId;
                ScrollRequest scrollRequest = ScrollRequest.of(sr -> sr
                        .scrollId(currentScrollId)
                        .scroll(Time.of(t -> t.time("1m"))));

                ScrollResponse<Map> scrollResponse = client.scroll(scrollRequest, Map.class);
                scrollId = scrollResponse.scrollId();
                hits = scrollResponse.hits().hits();
            }

            log.info("Finished scrolling. Found {} unique category names from approximately {} products scanned.",
                    uniqueCategoryNames.size(), totalProductsScanned);

            // index unique categories
            log.info("Indexing {} unique categories...", uniqueCategoryNames.size());
            int categoriesIndexed = 0;
            int categoriesSkipped = 0;
            for (String categoryName : uniqueCategoryNames) {
                Integer categoryId = Utils.CATEGORY_ID_MAP.get(categoryName);
                if (categoryId != null) {
                    Map<String, Object> categoryDoc = new HashMap<>();
                    categoryDoc.put("category_name", categoryName);
                    categoryDoc.put("category_id", categoryId);

                    final String docId = String.valueOf(categoryId);
                    log.debug("Indexing category: name='{}', id={}", categoryName, categoryId);
                    client.index(i -> i
                            .index("category")
                            .id(docId)
                            .document(categoryDoc));
                    categoriesIndexed++;
                }
            }
            log.info("Category indexing finished. Indexed: {}, Skipped (due to missing map entry): {}",
                    categoriesIndexed, categoriesSkipped);

        } catch (IOException e) {
            log.error("IOException during Elasticsearch operation: {}", e.getMessage(), e);
        } catch (Exception e) {
            log.error("Unexpected error during category extraction: {}", e.getMessage(), e);
        } finally {
            if (scrollId != null) {
                try {
                    if (restClient != null) {
                        RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
                        ElasticsearchClient client = new ElasticsearchClient(transport);

                        log.debug("Clearing scroll context ID: {}", scrollId);
                        final String finalScrollId = scrollId;
                        ClearScrollRequest clearScrollRequest = ClearScrollRequest.of(c -> c.scrollId(finalScrollId));
                        client.clearScroll(clearScrollRequest);
                        log.info("Scroll context cleared successfully.");
                    }
                } catch (Exception e) {
                    log.error("Error clearing scroll context {}: {}", scrollId, e.getMessage(), e);
                }
            }
        }
        log.info("Category extraction process finished.");
    }
}