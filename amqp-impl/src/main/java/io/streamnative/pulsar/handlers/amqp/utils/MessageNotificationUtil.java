package io.streamnative.pulsar.handlers.amqp.utils;

import java.util.HashMap;
import java.util.Map;
import org.apache.bookkeeper.mledger.Position;

public class MessageNotificationUtil {
    private static final String WEB_HOOK =
            "https://open.feishu.cn/open-apis/bot/v2/hook/64e1d9b4-95c8-4c94-9475-eceeb124d7b5";

    public static void routeQueueFailed(Position position, Throwable throwable, String name) {
        Map<String, Object> params = new HashMap<>(4);
        Map<String, String> content = new HashMap<>(2);
        content.put("text",
                String.format("exchange {%s} route queue position {%s} transaction rollback failed, exception: %s",
                        name, position.toString(), throwable));
        params.put("msg_type", "text");
        params.put("content", content);
        HttpUtil.postAsync(WEB_HOOK, params);
    }

    public static void resetTtlFailed(String queue, long expireTime, Throwable throwable) {
        Map<String, Object> params = new HashMap<>(4);
        Map<String, String> content = new HashMap<>(2);
        content.put("text",
                String.format("queue {%s} failed to reset topic ttl, current expireTime:{%s}, exception %s",
                        queue, expireTime, throwable));
        params.put("msg_type", "text");
        params.put("content", content);
        HttpUtil.postAsync(WEB_HOOK, params);
    }

    public static void exchangeRoutedQueueFailed(String exchange, Throwable throwable) {
        Map<String, Object> params = new HashMap<>(4);
        Map<String, String> content = new HashMap<>(2);
        content.put("text",
                String.format("Exchange {%s} failed to route queue topic, exception %s", exchange, throwable));
        params.put("msg_type", "text");
        params.put("content", content);
        HttpUtil.postAsync(WEB_HOOK, params);
    }

    public static void exchangeRoutingQueueFailed(String exchange, String position, Throwable throwable) {
        Map<String, Object> params = new HashMap<>(4);
        Map<String, String> content = new HashMap<>(2);
        content.put("text",
                String.format("Exchange {%s} failed to routing queue topic, position {%s} exception %s",
                        exchange, position, throwable));
        params.put("msg_type", "text");
        params.put("content", content);
        HttpUtil.postAsync(WEB_HOOK, params);
    }


    public static void main(String[] args) {
        resetTtlFailed("queue", 100000000, new RuntimeException("test ex"));
    }
}
