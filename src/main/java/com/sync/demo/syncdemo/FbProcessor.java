package com.sync.demo.syncdemo;

import com.sync.demo.rest.demorest.DownloadRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Collections;
import java.util.Map;

@Slf4j
@Component
public class FbProcessor {

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

    @Autowired
    private KafkaTemplate<String, DownloadResponse> kafkaTemplate;

    @KafkaListener(topics = "download-request", groupId = "download-request", containerFactory = "kafkaListenerContainerFactory")
    public void process(DownloadRequest message) {
        log.info("Start job {} with ids {}", message.getJobId(), message.getIds());
        Map<String, Object> response = request(message);
        log.info("Got response job {}", message.getJobId());
        message.getIds().stream()
                .map(id -> {
                    Object data = response.get(id);
                    return new DownloadResponse(message.getJobId(), id, data != null ? Status.SUCCESS : Status.MISSED, data);
                })
                .forEach(r -> kafkaTemplate.send("downloaded-ads", r));
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
                .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})
                .onErrorReturn(Collections.emptyMap())
                .block();
    }

    private ExchangeFilterFunction logRequest() {
        return (clientRequest, next) -> {
            log.info("Request: {} {}", clientRequest.method(), clientRequest.url());
//            clientRequest.headers()
//                    .forEach((name, values) -> values.forEach(value -> log.info("{}={}", name, value)));
            return next.exchange(clientRequest);
        };
    }


}
