package com.alibaba.datax.plugin.reader.mongodbreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.mongodbreader.KeyConstant;
import com.alibaba.datax.plugin.reader.mongodbreader.MongoDBReaderErrorCode;
import com.google.common.base.Strings;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by jianying.wcj on 2015/3/19 0019.
 * Modified by mingyan.zc on 2016/6/13.
 * Modified by mingyan.zc on 2017/7/5.
 */
public class CollectionSplitUtil {
    private static final Logger LOGGER = Loggers.getLogger("split-util");

    public static List<Configuration> doSplit(
        Configuration originalSliceConfig, int adviceNumber, MongoClient mongoClient) {

        List<Configuration> confList = new ArrayList<Configuration>();

        String dbName = originalSliceConfig.getString(KeyConstant.MONGO_DB_NAME, originalSliceConfig.getString(KeyConstant.MONGO_DATABASE));

        String collName = originalSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);

        String query = originalSliceConfig.getString(KeyConstant.MONGO_QUERY);

        if(Strings.isNullOrEmpty(dbName) || Strings.isNullOrEmpty(collName) || mongoClient == null) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
        }

        boolean isObjectId = isPrimaryIdObjectId(mongoClient, dbName, collName);

        List<Range> rangeList = doSplitCollection(adviceNumber, mongoClient, dbName, collName, isObjectId, query);
        for(Range range : rangeList) {
            Configuration conf = originalSliceConfig.clone();
            conf.set(KeyConstant.LOWER_BOUND, range.lowerBound);
            conf.set(KeyConstant.UPPER_BOUND, range.upperBound);
            conf.set(KeyConstant.IS_OBJECTID, isObjectId);
            confList.add(conf);
        }
        LOGGER.info("debug 5");
        return confList;
    }


    private static boolean isPrimaryIdObjectId(MongoClient mongoClient, String dbName, String collName) {
        MongoDatabase database = mongoClient.getDatabase(dbName);
        MongoCollection<Document> col = database.getCollection(collName);
        Document doc = col.find().limit(1).first();
        Object id = doc.get(KeyConstant.MONGO_PRIMARY_ID);
        if (id instanceof ObjectId) {
            return true;
        }
        return false;
    }

    // split the collection into multiple chunks, each chunk specifies a range
    private static List<Range> doSplitCollection(int adviceNumber, MongoClient mongoClient,
                                                 String dbName, String collName, boolean isObjectId, String query) {


        LOGGER.info("获取数据库");
        MongoDatabase database = mongoClient.getDatabase(dbName);
        List<Range> rangeList = new ArrayList<Range>();
        LOGGER.info("判断是否不拆分");
        if (adviceNumber == 1) {
            Range range = new Range();
            range.lowerBound = "min";
            range.upperBound = "max";
            return Arrays.asList(range);
        }

        LOGGER.info("获取集合状态及总数");
        int docCount = 0;
        Document queryFilter = null;
        if(StringUtils.isNoneBlank(query)){
            queryFilter = Document.parse(query);
            docCount = (int) database.getCollection(collName).countDocuments(queryFilter);
        } else {
            Document result = database.runCommand(new Document("collStats", collName));
            docCount = result.getInteger("count");
        }
        LOGGER.info("query: " + query);
        LOGGER.info("queryFilter: " + queryFilter.toJson());

        LOGGER.info("集合总数为: "+ docCount);
        if (docCount == 0) {
            return rangeList;
        }


        LOGGER.info("开始拆分集合");
        int splitPointCount = adviceNumber - 1;
        int chunkDocCount = docCount / adviceNumber;
        ArrayList<Object> splitPoints = new ArrayList<Object>();


        int skipCount = chunkDocCount;
        MongoCollection<Document> col = database.getCollection(collName);
        Document firstRow;
        if(queryFilter != null){
            firstRow = col.find(queryFilter).sort(Document.parse("{_id: 1}")).limit(1).first();
        } else {
            firstRow = col.find().sort(Document.parse("{_id: 1}")).limit(1).first();
        }

        if(firstRow == null){
            throw new RuntimeException("空集合");
        }
        Object minId = firstRow.get(KeyConstant.MONGO_PRIMARY_ID);

        if (isObjectId) {
            ObjectId oid = (ObjectId)minId;
            splitPoints.add(oid.toHexString());
        } else {
            splitPoints.add(minId);
        }

        Document doc;
        for (int i = 0; i < splitPointCount; i++) {
            // 使用skip进行拆分，这就是慢的根源，并且find没有指定query

            if (queryFilter != null){
                doc = col.find(queryFilter).skip(skipCount).limit(chunkDocCount).first();
            } else {
                doc = col.find().skip(skipCount).limit(chunkDocCount).first();
            }
            if(doc == null) {
                continue;
            }
            Object id = doc.get(KeyConstant.MONGO_PRIMARY_ID);
            if (isObjectId) {
                ObjectId oid = (ObjectId)id;
                splitPoints.add(oid.toHexString());
            } else {
                splitPoints.add(id);
            }
            skipCount += chunkDocCount;
        }


        Document lastRow;

        if (queryFilter != null) {
            lastRow = col.find(queryFilter).sort(Document.parse("{_id: -1}")).limit(1).first();
        } else {
            lastRow = col.find().sort(Document.parse("{_id: -1}")).limit(1).first();
        }

        if(lastRow == null) {
            throw new RuntimeException("空集合");
        }

        Object maxId = lastRow.get(KeyConstant.MONGO_PRIMARY_ID);
        if (isObjectId) {
            ObjectId oid = (ObjectId)maxId;
            splitPoints.add(oid.toHexString());
        } else {
            splitPoints.add(maxId);
        }

        Object lastObjectId = null;
        for (Object splitPoint : splitPoints) {
            if(lastObjectId != null){
                Range range = new Range();
                range.lowerBound = lastObjectId;
                lastObjectId = splitPoint;
                range.upperBound = lastObjectId;
                rangeList.add(range);
            } else {
                lastObjectId = splitPoint;
            }
        }

        return rangeList;
    }
}

class Range {
    Object lowerBound;
    Object upperBound;
}
