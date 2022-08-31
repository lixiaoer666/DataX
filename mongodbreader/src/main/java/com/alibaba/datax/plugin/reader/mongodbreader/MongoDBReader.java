package com.alibaba.datax.plugin.reader.mongodbreader;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.mongodbreader.util.CollectionSplitUtil;
import com.alibaba.datax.plugin.reader.mongodbreader.util.MongoUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import com.mongodb.client.model.Filters;
import org.mortbay.log.Log;

/**
 * Created by jianying.wcj on 2015/3/19 0019.
 * Modified by mingyan.zc on 2016/6/13.
 * Modified by mingyan.zc on 2017/7/5.
 */
public class MongoDBReader extends Reader {

    public static class Job extends Reader.Job {

        private Configuration originalConfig = null;

        private MongoClient mongoClient;

        private String userName = null;
        private String password = null;

        @Override
        public List<Configuration> split(int adviceNumber) {
            return CollectionSplitUtil.doSplit(originalConfig,adviceNumber,mongoClient);
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.userName = originalConfig.getString(KeyConstant.MONGO_USER_NAME, originalConfig.getString(KeyConstant.MONGO_USERNAME));
            this.password = originalConfig.getString(KeyConstant.MONGO_USER_PASSWORD, originalConfig.getString(KeyConstant.MONGO_PASSWORD));
            String database =  originalConfig.getString(KeyConstant.MONGO_DB_NAME, originalConfig.getString(KeyConstant.MONGO_DATABASE));
            String authDb =  originalConfig.getString(KeyConstant.MONGO_AUTHDB, database);
            if(!Strings.isNullOrEmpty(this.userName) && !Strings.isNullOrEmpty(this.password)) {
                this.mongoClient = MongoUtil.initCredentialMongoClient(originalConfig,userName,password,authDb);
            } else {
                this.mongoClient = MongoUtil.initMongoClient(originalConfig);
            }
        }

        @Override
        public void destroy() {

        }
    }


    public static class Task extends Reader.Task {

        private Configuration readerSliceConfig;

        private MongoClient mongoClient;

        private String userName = null;
        private String password = null;

        private String authDb = null;
        private String database = null;
        private String collection = null;

        private String query = null;

        private JSONArray mongodbColumnMeta = null;
        private Object lowerBound = null;
        private Object upperBound = null;
        private String timestampCol = null;
        private String timestampFmt = null;
        private SimpleDateFormat formatter = null;
        private String timeStrCol = null;
        private String timeStrFmt = null;
        private SimpleDateFormat timeStrFormatter = null;
        private boolean isObjectId = true;

        @Override
        public void startRead(RecordSender recordSender) {

            if(lowerBound== null || upperBound == null ||
                mongoClient == null || database == null ||
                collection == null  || mongodbColumnMeta == null) {
                throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                    MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
            }
            MongoDatabase db = mongoClient.getDatabase(database);
            MongoCollection col = db.getCollection(this.collection);

            MongoCursor<Document> dbCursor = null;
            Bson filter = new Document();
            if (lowerBound.equals("min")) {
                if (!upperBound.equals("max")) {
                    filter = Filters.lte(KeyConstant.MONGO_PRIMARY_ID, isObjectId ?  new ObjectId(upperBound.toString()) : upperBound);
                }
            } else if (upperBound.equals("max")) {
                filter = Filters.gte(KeyConstant.MONGO_PRIMARY_ID, isObjectId ? new ObjectId(lowerBound.toString()) : lowerBound);
            } else {
                filter = Filters.and(Filters.lte(KeyConstant.MONGO_PRIMARY_ID, new ObjectId(upperBound.toString())), Filters.gte(KeyConstant.MONGO_PRIMARY_ID, isObjectId ? new ObjectId(lowerBound.toString()) : lowerBound));
            }
            if(!Strings.isNullOrEmpty(query)) {
                Document queryFilter = Document.parse(query);
                filter = Filters.and(Arrays.asList(filter, queryFilter));
            }
            dbCursor = col.find(filter).iterator();
            while (dbCursor.hasNext()) {
                Document item = dbCursor.next();
                Record record = recordSender.createRecord();
                Iterator<Object> columnItera = mongodbColumnMeta.iterator();
                while (columnItera.hasNext()) {
                    JSONObject column = (JSONObject)columnItera.next();
                    Object tempCol = item.get(column.getString(KeyConstant.COLUMN_NAME));
                    if (tempCol == null) {
                        if (KeyConstant.isDocumentType(column.getString(KeyConstant.COLUMN_TYPE))) {
                            String[] name = column.getString(KeyConstant.COLUMN_NAME).split("\\.");
                            if (name.length > 1) {
                                Object obj;
                                Document nestedDocument = item;
                                for (String str : name) {
                                    obj = nestedDocument.get(str);
                                    if (obj instanceof Document) {
                                        nestedDocument = (Document) obj;
                                    }
                                }

                                if (null != nestedDocument) {
                                    Document doc = nestedDocument;
                                    tempCol = doc.get(name[name.length - 1]);
                                }
                            }
                        }
                    }
                    if (tempCol == null) {
                        //continue; 这个不能直接continue会导致record到目的端错位
                        record.addColumn(new StringColumn(null));
                    } else if (formatter != null && timestampCol.equals(column.getString(KeyConstant.COLUMN_NAME))) {
                        // 如果当前字段是时间戳字段，根据配置format为时间字符串
                        // 10位时间戳自动补0
                        String ts = tempCol.toString();
                        record.addColumn(new StringColumn(formatter.format(new Date(Long.parseLong(ts.length() == 10 ? ts + "000" : ts)))));
                    } else if (timeStrFormatter != null && timeStrCol.equals(column.getString(KeyConstant.COLUMN_NAME))) {
                        // 如果当前字段是字符串时间，根据配置format为时间字符串
                        String ts = tempCol.toString();
                        record.addColumn(new DateColumn(timeStrFormatter.parse(ts, new ParsePosition(0))));
                    } else if (tempCol instanceof Double) {
                        //TODO deal with Double.isNaN()
                        record.addColumn(new DoubleColumn((Double) tempCol));
                    } else if (tempCol instanceof Boolean) {
                        record.addColumn(new BoolColumn((Boolean) tempCol));
                    } else if (tempCol instanceof Date) {
                        record.addColumn(new DateColumn((Date) tempCol));
                    } else if (tempCol instanceof Integer) {
                        record.addColumn(new LongColumn((Integer) tempCol));
                    } else if (tempCol instanceof Long) {
                        record.addColumn(new LongColumn((Long) tempCol));
                    } else if(KeyConstant.isArrayType(column.getString(KeyConstant.COLUMN_TYPE))) {
                        String splitter = column.getString(KeyConstant.COLUMN_SPLITTER);
                        if(Strings.isNullOrEmpty(splitter)) {
                            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                                    MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
                        } else {
                            ArrayList array = (ArrayList)tempCol;
                            String tempArrayStr = Joiner.on(splitter).join(array);
                            record.addColumn(new StringColumn(tempArrayStr));
                        }
                    } else if(KeyConstant.isJsonType(column.getString(KeyConstant.COLUMN_TYPE))) {
                        ArrayList array = (ArrayList)tempCol;
                        record.addColumn(new StringColumn(JSON.toJSONString(array)));
                    } else {
                        record.addColumn(new StringColumn(tempCol.toString()));
                    }
                }
                recordSender.sendToWriter(record);
            }
        }

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            this.userName = readerSliceConfig.getString(KeyConstant.MONGO_USER_NAME, readerSliceConfig.getString(KeyConstant.MONGO_USERNAME));
            this.password = readerSliceConfig.getString(KeyConstant.MONGO_USER_PASSWORD, readerSliceConfig.getString(KeyConstant.MONGO_PASSWORD));
            this.database = readerSliceConfig.getString(KeyConstant.MONGO_DB_NAME, readerSliceConfig.getString(KeyConstant.MONGO_DATABASE));
            this.timestampCol = readerSliceConfig.getString(KeyConstant.TIMESTAMP_COLUMN);
            this.timestampFmt = readerSliceConfig.getString(KeyConstant.TIMESTAMP_FORMAT);
            if(!Strings.isNullOrEmpty(this.timestampCol) && !Strings.isNullOrEmpty(this.timestampFmt)) {
                this.formatter = new SimpleDateFormat(this.timestampFmt);
            }
            this.timeStrCol = readerSliceConfig.getString(KeyConstant.TIME_STR_COLUMN);
            this.timeStrFmt = readerSliceConfig.getString(KeyConstant.TIME_STR_FORMAT);
            if(!Strings.isNullOrEmpty(this.timeStrCol) && !Strings.isNullOrEmpty(this.timeStrFmt)) {
                this.timeStrFormatter = new SimpleDateFormat(this.timeStrFmt);
            }
            this.authDb = readerSliceConfig.getString(KeyConstant.MONGO_AUTHDB, this.database);
            if(!Strings.isNullOrEmpty(userName) && !Strings.isNullOrEmpty(password)) {
                mongoClient = MongoUtil.initCredentialMongoClient(readerSliceConfig,userName,password,authDb);
            } else {
                mongoClient = MongoUtil.initMongoClient(readerSliceConfig);
            }

            this.collection = readerSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);
            this.query = readerSliceConfig.getString(KeyConstant.MONGO_QUERY);
            this.mongodbColumnMeta = JSON.parseArray(readerSliceConfig.getString(KeyConstant.MONGO_COLUMN));
            this.lowerBound = readerSliceConfig.get(KeyConstant.LOWER_BOUND);
            this.upperBound = readerSliceConfig.get(KeyConstant.UPPER_BOUND);
            this.isObjectId = readerSliceConfig.getBool(KeyConstant.IS_OBJECTID);
        }

        @Override
        public void destroy() {

        }

    }
}
