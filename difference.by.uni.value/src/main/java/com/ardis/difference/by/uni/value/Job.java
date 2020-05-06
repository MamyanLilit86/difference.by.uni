package com.ardis.difference.by.uni.value;

import com.mongodb.client.MongoClients;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class Job {

    private static long withUni, currentOldDataSize, lostDataCount;
    private static DifferenceConfig differenceConfig = new DifferenceConfig();
    private static String collection;

    public Job(@Value("${mongo.db.new}") String databaseNew,
               @Value("${mongo.db.old}") String databaseOld,
               @Value("${mongo.db.diff}") String databaseDiff,
               @Value("${mongo.col}") String collection) {
        this.collection = collection;
        var mongoClient = MongoClients.create();
        var mongoTemplateNew = new MongoTemplate(mongoClient, databaseNew);
        var mongoTemplateResult = new MongoTemplate(
                mongoClient, databaseDiff);
        mongoTemplateResult.dropCollection(collection);
        new MongoTemplate(
                mongoClient, databaseOld)
                .stream(new Query(), Document.class, collection)
                .forEachRemaining(item -> checkCoincidence(item, mongoTemplateNew, mongoTemplateResult));

        log.info("Data With Uni = {}", withUni);
        log.info("Data Without Uni = {}", currentOldDataSize - withUni);
        log.info("Old Data Size = {}", currentOldDataSize);
        log.info("Coincidence Data Count = {}", withUni - lostDataCount);
        log.info("Lost Data Count = {}", lostDataCount);
    }

    public static void checkCoincidence(Document item, MongoTemplate mongoTemplateNew, MongoTemplate mongoTemplateResult) {
        currentOldDataSize++;
        if (item.containsKey("uni")) {
            withUni++;
            List<String> oldDataUniValueTemp = new ArrayList<>();
            item.getList("uni", Document.class)
                    .forEach(i -> oldDataUniValueTemp.add((String) i.get("value")));
            Query queryNew = new Query().addCriteria(Criteria.where("uni.value").in(oldDataUniValueTemp));
            if (!mongoTemplateNew.stream(queryNew, Document.class, collection).hasNext()) {
                mongoTemplateResult.insert(item, collection);
                lostDataCount++;
            }
        }
    }
}

