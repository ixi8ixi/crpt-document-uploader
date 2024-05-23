package com.anwhiteko;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;

import java.io.Closeable;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class CrptApi implements Closeable {
    private static final String CRPT_URL = "https://ismp.crpt.ru/api/v3/lk/documents/create";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final RateLimiter rateLimiter;
    private final CloseableHttpClient httpClient;

    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        if (timeUnit == TimeUnit.NANOSECONDS || timeUnit == TimeUnit.MICROSECONDS || timeUnit == TimeUnit.MILLISECONDS) {
            throw new IllegalArgumentException("Invalid time unit");
        }

        if (requestLimit < 1) {
            throw new IllegalArgumentException("Request limit should be positive");
        }

        this.rateLimiter = new RateLimiter(timeUnit, requestLimit);
        this.httpClient = HttpClients.createDefault();
    }

    public String sendDocument(Document document, String signature) throws InterruptedException {
        rateLimiter.joinQueue();

        HttpEntity entity = new StringEntity(documentToJson(document), ContentType.APPLICATION_JSON);
        HttpPost request = new HttpPost(CRPT_URL);
        request.setHeader("signature", signature);
        request.setEntity(entity);

        try (CloseableHttpResponse response = httpClient.execute(request)) {
            HttpEntity responseEntity = response.getEntity();
            String result = EntityUtils.toString(responseEntity);
            EntityUtils.consume(responseEntity);
            return result;
        } catch (IOException | ParseException e) {
            throw new IllegalStateException("Problems sending a request");
        }
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }

    private String documentToJson(Document document) {
        try {
            return OBJECT_MAPPER.writeValueAsString(document);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid document", e);
        }
    }

    public record Description(String participantInn) {}

    public record Product(
            String certificate_document,
            LocalDate certificate_document_date,
            String certificate_document_number,
            String owner_inn,
            String producer_inn,
            LocalDate production_date,
            String tnved_code,
            String uit_code,
            String uitu_code
    ) {}

    @Builder
    public record Document(
            Description description,
            String doc_id,
            String doc_status,
            String doc_type,
            boolean importRequest,
            String owner_inn,
            String participant_inn,
            String producer_inn,
            LocalDate production_date,
            String production_type,
            List<Product> products,
            LocalDate reg_date,
            String reg_number
    ) {}

    private static class RateLimiter {
        private final long timeUnit;
        private final int requestLimit;
        private final ArrayDeque<Long> times;

        public RateLimiter(TimeUnit timeUnit, int requestLimit) {
            this.timeUnit = timeUnit.toMillis(1);
            this.requestLimit = requestLimit;
            this.times = new ArrayDeque<>(requestLimit);
        }

        synchronized public void joinQueue() throws InterruptedException {
            long now = System.currentTimeMillis();

            if (times.size() < requestLimit) {
                times.addFirst(now);
                return;
            }

            long border = now - timeUnit;
            long last = times.removeLast();
            if (last <= border) {
                times.addFirst(now);
            } else {
                long trueLast = last + timeUnit;
                times.addFirst(trueLast);
                wait(trueLast - now);
            }
        }
    }
}
