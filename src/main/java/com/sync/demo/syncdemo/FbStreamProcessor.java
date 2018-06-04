package com.sync.demo.syncdemo;

import com.sync.demo.rest.demorest.DownloadRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsProcessor;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author rbezpalko
 * @since 22.05.18
 */

@EnableBinding(KafkaStreamsProcessor.class)
@Slf4j
public class FbStreamProcessor {

    AtomicInteger counter = new AtomicInteger(0);

    public static final String ASSET_FEED_FIELDS = "asset_feed_id,asset_feed_spec"+
            "{" +
            "videos" +
            "{" +
            "video_id,thumbnail_hash,thumbnail_url,adlabels"+
            "}" +
            ",ad_formats,titles,link_urls,descriptions,call_to_action_types,images" +
            "{" +
            "hash,url,image_crops,adlabels" +
            "}" +
            ",bodies,optimization_type,asset_customization_rules" +
            "}";


    public static final String AD_CREATIVE_FIELDS = "id,body,image_hash,image_url,"
            + "object_url,name,title,object_id,"
            + "url_tags,effective_object_story_id,object_story_id,call_to_action_type,video_id,"
            + "object_type,image_crops,auto_update,link_url,object_story_spec,"
            + "instagram_actor_id,template_url,instagram_permalink_url,use_page_actor_override,applink_treatment,"
            + "link_destination_display_url,product_set_id,template_url_spec,dynamic_ad_voice,platform_customizations,authorization_category,"
            + ASSET_FEED_FIELDS;

    public static final String AD_GROUP_FIELDS = "id,account_id,effective_status,adset_id,"
            + "creative" +
            "{" + AD_CREATIVE_FIELDS + "}" +
            ",name,tracking_specs,"
            + "updated_time,created_time,view_tags,ad_review_feedback,bid_amount";

    @StreamListener("input")
    @SendTo("output")
    public KStream<?, DownloadResponse> process(KStream<Object, DownloadRequest> input) {


        return input
                .flatMapValues(value -> {
                    log.info("Start inc {} job {} with ids {} threadId {}", counter.incrementAndGet(), value.getJobId(), value.getIds(), Thread.currentThread().getId());
                    Map<String, Object> response = request(value);
                    return value.getIds().stream()
                            .map(id -> {
                                Object data = response.get(id);
                                return new DownloadResponse(value.getJobId(), id, data != null ? Status.SUCCESS : Status.MISSED, data); })
                            .collect(Collectors.toList());
                });
    }

    private Map<String, Object> request(DownloadRequest request) {
        return  WebClient.builder()
                .baseUrl("https://graph.facebook.com/v2.11/")
                .filter(logRequest()).build().get()
                .uri(builder -> builder
                        .queryParam("ids", request.getIds())
                        .queryParam("fields", "{fields}")
                        .queryParam("access_token", "{token}")
                        .build(AD_GROUP_FIELDS, request.getToken()))
                .retrieve()
                .bodyToFlux(new ParameterizedTypeReference<Map<String, Object>>() {})
                .onErrorReturn(Collections.emptyMap())
                .blockLast();
    }

    private static ExchangeFilterFunction logRequest() {
        //            log.info("Request: {} {}", clientRequest.method(), clientRequest.url());
//            clientRequest.headers().forEach((name, values) -> values.forEach(value -> log.info("{}={}", name, value)));
        return ExchangeFilterFunction.ofRequestProcessor(Mono::just);
    }


}
