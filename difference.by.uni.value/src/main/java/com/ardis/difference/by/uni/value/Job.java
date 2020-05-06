package com.ardis.difference.by.uni.value;

import com.mongodb.client.MongoClients;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
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

    public Job() {
        var mongoClient = MongoClients.create();
        var mongoTemplateNew = new MongoTemplate(mongoClient, differenceConfig.getDatabaseNew());
        var mongoTemplateResult = new MongoTemplate(
                mongoClient, differenceConfig.getDatabaseDif());
        mongoTemplateResult.dropCollection(differenceConfig.getCollection());
        new MongoTemplate(
                mongoClient, differenceConfig.getDatabaseOld())
                .stream(new Query(), Document.class, differenceConfig.getCollection())
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
            if (!mongoTemplateNew.stream(queryNew, Document.class, differenceConfig.getCollection()).hasNext()) {
                mongoTemplateResult.insert(item, differenceConfig.getCollection());
                lostDataCount++;
            }
        }
    }
}

