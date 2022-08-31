
package com.alibaba.datax.plugin.writer.starrockswriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class StarRocksWriter extends Writer {
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);
        private Configuration originalConfig;


        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

        }


        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            for (int i = 0; i < mandatoryNumber; i++) {
                writerSplitConfigs.add(this.originalConfig);
            }

            return writerSplitConfigs;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);

        private static final int ERROR_LOG_MAX_LENGTH = 3000;
        private Configuration readerSliceConfig;
        private List<Object> addressList;
        private String database;
        private String table;
        private String username;
        private String password;
        private String labelPrefix;
        private String format;
        private String rowDelimiter;
        private String columns;
        private String jsonColumns;
        private List<String> columnList;
        private List<String> jsonColumnList;
        private List<byte[]> buffer;
        private int batchSize;

        private String jsonpaths;

        private SimpleDateFormat sdf;

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            this.database = this.readerSliceConfig.getString(Key.DATABASE);
            this.table = this.readerSliceConfig.getString(Key.TABLE);
            this.username = this.readerSliceConfig.getString(Key.USERNAME);
            this.password = this.readerSliceConfig.getString(Key.PASSWORD, "");
            this.labelPrefix = this.readerSliceConfig.getString(Key.LABEL_PREFIX, Key.DEFAULT_LABEL_PREFIX);
            this.format = this.readerSliceConfig.getString(Key.FORMAT, Key.DEFAULT_FORMAT);
            this.rowDelimiter = this.readerSliceConfig.getString(Key.ROW_DELIMITER, Key.DEFAULT_ROW_DELIMITER);
            this.columns = this.readerSliceConfig.getString(Key.COLUMNS);
            this.jsonColumns = this.readerSliceConfig.getString(Key.JSON_COLUMNS);
            this.addressList = this.readerSliceConfig.getList(Key.ADDRESS);
            if(StringUtils.isNotBlank(columns)) {
                this.columnList = Arrays.asList(this.columns.split(","));
            }
            if(StringUtils.isNotBlank(jsonColumns)) {
                this.jsonColumnList = Arrays.asList(this.jsonColumns.split(","));
            }
            this.batchSize = 0;
            this.buffer = new ArrayList<>();
            this.sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            LOG.info(this.format);
            this.jsonpaths = this.readerSliceConfig.getString(Key.JSON_PATHS);
        }

        @Override
        public void prepare() {
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            Record record;
            while ((record = recordReceiver.getFromReader()) != null) {
                if (Key.JSON.equals(this.format)) {
                    JSONObject body = new JSONObject();
                    for (int i = 0; i < columnList.size(); i++) {
                        if (record.getColumn(i).getType().equals(Column.Type.DATE)) {
                            body.put(columnList.get(i), sdf.format(record.getColumn(i).asDate()));
                        } else if (this.jsonColumnList.contains(columnList.get(i))) {
                            body.put(columnList.get(i), JSONObject.parse(record.getColumn(i).getRawData().toString()));
                        } else {
                            body.put(columnList.get(i), record.getColumn(i).getRawData());
                        }
                    }
                    byte[] data = body.toJSONString().getBytes();
//                    LOG.info(body.toJSONString());
                    buffer.add(data);
                    batchSize += data.length;
                } else if (Key.CSV.equals(this.format)) {
                    byte[] data = recordToCsv(record).getBytes();
                    buffer.add(data);
                    batchSize += data.length;
                }
                if (batchSize >= Key.FLUSH_BYTE_SIZE || buffer.size() >= Key.FLUSH_COUNT) {
                    String label = this.labelPrefix + UUID.randomUUID().toString();
                    for (int i = 0; i < Key.MAX_RETRY; i++) {
                        try {
                            doStreamLoad(label);
                        } catch (Exception e) {
                            LOG.warn("Failed to flush batch data to StarRocks, retry times = {}", i, e);
                        }
                    }
                    batchSize = 0;
                    buffer.clear();
                }
            }
        }


        @Override
        public void post() {
        }

        @Override
        public void destroy() {
            if(batchSize != 0){
                String label = this.labelPrefix + UUID.randomUUID().toString();
                for (int i = 0; i < Key.MAX_RETRY; i++) {
                    try {
                        doStreamLoad(label);
                    } catch (Exception e) {
                        LOG.warn("Failed to flush batch data to StarRocks, retry times = {}", i, e);
                    }
                }
                batchSize = 0;
                buffer.clear();
            }
        }

        public Map<String, Object> doStreamLoad(String label) throws IOException {
            String host = getAvailableHost();
            if (null == host) {
                throw new IOException("None of the hosts in `load_url` could be connected.");
            }
            String loadUrl = new StringBuilder(host)
                    .append("/api/")
                    .append(this.database)
                    .append("/")
                    .append(this.table)
                    .append("/_stream_load")
                    .toString();
//            LOG.info(String.format("Start to join batch data: label[%s].", label));
//            LOG.info(String.format("loadUrl: %s", loadUrl));
//            LOG.info(String.format("buffer: %s", buffer.toString()));
//            LOG.info(String.format("size: %d", batchSize));
            Map<String, Object> loadResult = doHttpPut(loadUrl, label, joinRows(buffer,  batchSize));
            final String keyStatus = "Status";
            if (null == loadResult || !loadResult.containsKey(keyStatus)) {
                throw new IOException("Unable to flush data to StarRocks: unknown result status, usually caused by: 1.authorization or permission related problems. 2.Wrong column_separator or row_delimiter. 3.Column count exceeded the limitation.");
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Stream Load response: \n%s\n", JSON.toJSONString(loadResult)));
            }
            if (Key.RESULT_FAILED.equals(loadResult.get(keyStatus))) {
                Map<String, String> logMap = new HashMap<>();
                if (loadResult.containsKey("ErrorURL")) {
                    logMap.put("streamLoadErrorLog", getErrorLog((String) loadResult.get("ErrorURL")));
                }
                throw new StarRocksStreamLoadFailedException(String.format("Failed to flush data to StarRocks, Error " +
                        "response: \n%s\n%s\n", JSON.toJSONString(loadResult), JSON.toJSONString(logMap)), loadResult);
            } else if (Key.RESULT_LABEL_EXISTED.equals(loadResult.get(keyStatus))) {
                LOG.error(String.format("Stream Load response: \n%s\n", JSON.toJSONString(loadResult)));
                // has to block-checking the state to get the final result
                checkLabelState(host, label);
            }
            return loadResult;
        }


        private String getBasicAuthHeader(String username, String password) {
            String auth = username + ":" + password;
            byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
            return new StringBuilder("Basic ").append(new String(encodedAuth)).toString();
        }

        private HttpEntity getHttpEntity(CloseableHttpResponse resp) {
            int code = resp.getStatusLine().getStatusCode();
            if (200 != code) {
                LOG.warn("Request failed with code:{}", code);
                return null;
            }
            HttpEntity respEntity = resp.getEntity();
            if (null == respEntity) {
                LOG.warn("Request failed with empty response.");
                return null;
            }
            return respEntity;
        }
        @SuppressWarnings("unchecked")
        private void checkLabelState(String host, String label) throws IOException {
            int idx = 0;
            while(true) {
                try {
                    TimeUnit.SECONDS.sleep(Math.min(++idx, 5));
                } catch (InterruptedException ex) {
                    break;
                }
                try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
                    HttpGet httpGet = new HttpGet(new StringBuilder(host).append("/api/").append(this.database).append("/get_load_state?label=").append(label).toString());
                    httpGet.setHeader("Authorization", getBasicAuthHeader(this.username, this.password));
                    httpGet.setHeader("Connection", "close");

                    try (CloseableHttpResponse resp = httpclient.execute(httpGet)) {
                        HttpEntity respEntity = getHttpEntity(resp);
                        if (respEntity == null) {
                            throw new StarRocksStreamLoadFailedException(String.format("Failed to flush data to StarRocks, Error " +
                                    "could not get the final state of label[%s].\n", label), null);
                        }
                        Map<String, Object> result = (Map<String, Object>)JSON.parse(EntityUtils.toString(respEntity));
                        String labelState = (String)result.get("state");
                        if (null == labelState) {
                            throw new StarRocksStreamLoadFailedException(String.format("Failed to flush data to StarRocks, Error " +
                                    "could not get the final state of label[%s]. response[%s]\n", label, EntityUtils.toString(respEntity)), null);
                        }
                        LOG.info(String.format("Checking label[%s] state[%s]\n", label, labelState));
                        switch(labelState) {
                            case Key.LAEBL_STATE_VISIBLE:
                            case Key.LAEBL_STATE_COMMITTED:
                                return;
                            case Key.RESULT_LABEL_PREPARE:
                                continue;
                            case Key.RESULT_LABEL_ABORTED:
                                throw new StarRocksStreamLoadFailedException(String.format("Failed to flush data to StarRocks, Error " +
                                        "label[%s] state[%s]\n", label, labelState), null, true);
                            case Key.RESULT_LABEL_UNKNOWN:
                            default:
                                throw new StarRocksStreamLoadFailedException(String.format("Failed to flush data to StarRocks, Error " +
                                        "label[%s] state[%s]\n", label, labelState), null);
                        }
                    }
                }
            }
        }


        @SuppressWarnings("unchecked")
        private Map<String, Object> doHttpPut(String loadUrl, String label, byte[] data) throws IOException {
            LOG.info(String.format("Executing stream load to: '%s', size: '%s', thread: %d", loadUrl, data.length, Thread.currentThread().getId()));
            final HttpClientBuilder httpClientBuilder = HttpClients.custom()
                    .setRedirectStrategy(new DefaultRedirectStrategy() {
                        @Override
                        protected boolean isRedirectable(String method) {
                            return true;
                        }
                    });
            try (CloseableHttpClient httpclient = httpClientBuilder.build()) {
                HttpPut httpPut = new HttpPut(loadUrl);

//                if (Key.CSV.equals(this.format)) {
                httpPut.setHeader("columns", this.columns);
//                }
                if (Key.JSON.equals(this.format)) {
                    httpPut.setHeader("jsonpaths", this.jsonpaths);
                    httpPut.setHeader("format", Key.JSON);
                    httpPut.setHeader("strip_outer_array", "true");
                }
                if (!httpPut.containsHeader("timeout")) {
                    httpPut.setHeader("timeout", "60");
                }
                httpPut.setHeader("Expect", "100-continue");
                httpPut.setHeader("label", label);
                httpPut.setHeader("Authorization", getBasicAuthHeader(this.username, this.password));
                httpPut.setEntity(new ByteArrayEntity(data));
                httpPut.setConfig(RequestConfig.custom().setRedirectsEnabled(true).build());
//                LOG.info(httpPut.toString());
//                for(Header header: httpPut.getAllHeaders()){
//                    LOG.info(header.getName()+":"+header.getValue());
//                }

                try (CloseableHttpResponse resp = httpclient.execute(httpPut)) {
                    HttpEntity respEntity = getHttpEntity(resp);
                    if (respEntity == null)
                        return null;
                    return (Map<String, Object>)JSON.parse(EntityUtils.toString(respEntity));
                }
            }
        }

        private String getErrorLog(String errorUrl) {
            if (errorUrl == null || !errorUrl.startsWith("http")) {
                return null;
            }
            try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
                HttpGet httpGet = new HttpGet(errorUrl);
                try (CloseableHttpResponse resp = httpclient.execute(httpGet)) {
                    HttpEntity respEntity = getHttpEntity(resp);
                    if (respEntity == null) {
                        return null;
                    }
                    String errorLog = EntityUtils.toString(respEntity);
                    if (errorLog != null && errorLog.length() > ERROR_LOG_MAX_LENGTH) {
                        errorLog = errorLog.substring(0, ERROR_LOG_MAX_LENGTH);
                    }
                    return errorLog;
                }
            } catch (Exception e) {
                LOG.warn("Failed to get error log.", e);
                return "Failed to get error log: " + e.getMessage();
            }
        }

        private byte[] joinRows(List<byte[]> rows, int totalBytes) throws IOException {
            if (Key.CSV.equals(this.format)) {
                byte[] lineDelimiter = StarRocksDelimiterParser.parse(this.rowDelimiter, "\n").getBytes(StandardCharsets.UTF_8);
                ByteBuffer bos = ByteBuffer.allocate(totalBytes + rows.size() * lineDelimiter.length);
                for (byte[] row : rows) {
                    bos.put(row);
                    bos.put(lineDelimiter);
                }
                return bos.array();
            }

            if (Key.JSON.equals(this.format)) {
                ByteBuffer bos = ByteBuffer.allocate(totalBytes + (rows.isEmpty() ? 2 : rows.size() + 1));
                bos.put("[".getBytes(StandardCharsets.UTF_8));
                byte[] jsonDelimiter = ",".getBytes(StandardCharsets.UTF_8);
                boolean isFirstElement = true;
                for (byte[] row : rows) {
                    if (!isFirstElement) {
                        bos.put(jsonDelimiter);
                    }
                    bos.put(row);
                    isFirstElement = false;
                }
                bos.put("]".getBytes(StandardCharsets.UTF_8));
                return bos.array();
            }
            throw new RuntimeException("Failed to join rows data, unsupported `format` from stream load properties:");
        }
        private String getAvailableHost() {
            long tmp = addressList.size();
            Random random = new Random();
            for (int i = 0; i < tmp; i++) {
                int index = random.nextInt(addressList.size());
                String host = new StringBuilder("http://").append(addressList.get(index)).toString();
                if (tryHttpConnection(host)) {
                    return host;
                } else {
                    addressList.remove(index);
                }
            }
            return null;
        }

        private String recordToCsv(Record record) {
            int recordLength = record.getColumnNumber();
            if (0 == recordLength) {
                return this.rowDelimiter;
            }

            Column column;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < recordLength; i++) {
                column = record.getColumn(i);
                if (column.getType().equals(Column.Type.DATE)){
                    sb.append(sdf.format(column.asDate())).append("\t");
                } else{
                    sb.append(column.asString()).append("\t");
                }
            }
            sb.setLength(sb.length() - 1);
            sb.append(this.rowDelimiter);
            return sb.toString();
        }

        private boolean tryHttpConnection(String host) {
            try {
                URL url = new URL(host);
                HttpURLConnection co =  (HttpURLConnection) url.openConnection();
                co.setConnectTimeout(300 * 1000);
                co.connect();
                co.disconnect();
                return true;
            } catch (Exception e1) {
                LOG.warn("Failed to connect to address:{}", host, e1);
                return false;
            }
        }
    }
}
