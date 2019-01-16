/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.example;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.traversal.optimize.Text;
import com.baidu.hugegraph.traversal.optimize.TraversalUtil;
import com.baidu.hugegraph.util.Log;

public class Example2 {

    private static final Logger LOG = Log.logger(Example2.class);

    public static void main(String[] args) throws InterruptedException {
        LOG.info("Example2 start!");

        HugeGraph graph = ExampleUtil.loadGraph(true, false);

        Example2.load(graph);
        traversal(graph);

        graph.close();

        HugeGraph.shutdown(30L);
    }

    public static void traversal(final HugeGraph graph) {
        GraphTraversalSource g = graph.traversal();
        GraphTraversal<Vertex, Vertex> traversal;

        // Query by label, DONE
        traversal = g.V().hasLabel("person", "software").has("~page", "").limit(1);
        System.out.println("第一页：" + traversal.next());
        String page = TraversalUtil.page(traversal);
        System.out.println("page: " + page);
        traversal = g.V().hasLabel("person", "software").has("~page", page).limit(19);
        while (traversal.hasNext()) {
            System.out.println(traversal.next());
        }

        // Query by single secondary index
        // 不分页，显示全部
//        System.out.println(">>>> name = marko vertices:");
//        System.out.println(traversal.has("name", "marko").toList());

//        // 单索引secondary
//        System.out.println(">>>> name = marko vertices:");
//        // 总共有3条，这里取两条，应该还剩一条
//        traversal = g.V().has("name", "marko").has("~page", "").limit(22);
//        // 为什么第二次hasNext的时候会出来一个null
//        while (traversal.hasNext()) {
//            System.out.println(traversal.next());
//        }
//        // 这里应该能取出下一个page的
//        String page = TraversalUtil.page(traversal);
//        System.out.println(page);
//        traversal = g.V().has("name", "marko").has("~page", page).limit(30);
//        while (traversal.hasNext()) {
//            System.out.println(traversal.next());
//        }

//        // 单索引range
//        System.out.println(">>>> price >= 100 && price < 300 vertices:");
//        traversal = g.V().has("price", P.between(100, 300)).has("~page", "").limit(2);
//        while (traversal.hasNext()) {
//            System.out.println(traversal.next());
//        }
//        // 单索引search
//        System.out.println(">>>> city contains Haidian vertices:");
//        traversal = g.V().has("city", Text.contains("Beijing Haidian")).has("~page", "").limit(2);
//        while (traversal.hasNext()) {
//            System.out.println(traversal.next());
//        }
    }

    public static void load(final HugeGraph graph) {
        SchemaManager schema = graph.schema();

        schema.propertyKey("name").asText().ifNotExist().create();
        schema.propertyKey("age").asInt().ifNotExist().create();
        schema.propertyKey("city").asText().ifNotExist().create();
        schema.propertyKey("weight").asDouble().ifNotExist().create();
        schema.propertyKey("lang").asText().ifNotExist().create();
        schema.propertyKey("date").asText().ifNotExist().create();
        schema.propertyKey("price").asInt().ifNotExist().create();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .useCustomizeStringId()
              .nullableKeys("age")
              .ifNotExist()
              .create();

        schema.vertexLabel("software")
              .properties("name", "lang", "price")
              .useCustomizeStringId()
              .nullableKeys("price")
              .ifNotExist()
              .create();

        schema.vertexLabel("programmer")
              .properties("name", "age", "city")
              .useCustomizeStringId()
              .nullableKeys("age")
              .ifNotExist()
              .create();

        schema.indexLabel("personByNameAndCity")
              .onV("person")
              .by("name", "city")
              .secondary()
              .ifNotExist()
              .create();

        schema.indexLabel("softwareByNameAndLang")
              .onV("software")
              .by("name", "lang")
              .ifNotExist()
              .create();

        schema.indexLabel("programmerByName")
              .onV("programmer")
              .by("name")
              .secondary()
              .ifNotExist()
              .create();

        schema.indexLabel("personByCity")
              .onV("person")
              .search()
              .by("city")
              .ifNotExist()
              .create();

        schema.indexLabel("softwareByPrice")
              .onV("software")
              .by("price")
              .range()
              .ifNotExist()
              .create();

        schema.edgeLabel("knows")
              .multiTimes()
              .sourceLabel("person")
              .targetLabel("person")
              .properties("date", "weight")
              .sortKeys("date")
              .nullableKeys("weight")
              .ifNotExist()
              .create();

        schema.edgeLabel("created")
              .sourceLabel("person").targetLabel("software")
              .properties("date", "weight")
              .nullableKeys("weight")
              .ifNotExist()
              .create();

//        schema.indexLabel("createdByDate")
//              .onE("created")
//              .by("date")
//              .secondary()
//              .ifNotExist()
//              .create();
//
//        schema.indexLabel("createdByWeight")
//              .onE("created")
//              .by("weight")
//              .range()
//              .ifNotExist()
//              .create();
//
//        schema.indexLabel("knowsByWeight")
//              .onE("knows")
//              .by("weight")
//              .range()
//              .ifNotExist()
//              .create();

        graph.tx().open();

        for (int i = 1; i <= 18; i++) {
            String id = "person_marko" + i;
            graph.addVertex(T.label, "person", T.id, id, "name", "marko", "age", 29, "city", "Beijing Chaoyang");
        }

        for (int i = 1; i <= 16; i++) {
            String id = "programmer_marko" + i;
            graph.addVertex(T.label, "programmer", T.id, id, "name", "marko", "age", 30, "city", "Beijing Haidian");
        }

        for (int i = 1; i <= 1; i++) {
            String id = "software_marko" + i;
            graph.addVertex(T.label, "software", T.id, id, "name", "marko", "lang", "java", "price", 100);
        }

//        Vertex p_marko = graph.addVertex(T.label, "person", T.id, "p_marko0", "name", "marko", "age", 29, "city", "Beijing Haidian");
//        Vertex vadas = graph.addVertex(T.label, "person", T.id, "vadas", "name", "vadas", "age", 27, "city", "Beijing Haidian");
//        Vertex josh = graph.addVertex(T.label, "person", T.id, "josh", "name", "josh", "age", 32, "city", "Beijing");
//        Vertex peter = graph.addVertex(T.label, "person", T.id, "peter", "name", "peter", "age", 35, "city", "Shanghai");
//        Vertex s_marko = graph.addVertex(T.label, "software", T.id, "s_marko", "name", "marko", "lang", "java", "price", 100);
//        Vertex ripple = graph.addVertex(T.label, "software", T.id, "ripple", "name", "ripple", "lang", "java", "price", 200);
//        Vertex hadoop = graph.addVertex(T.label, "software", T.id, "hadoop", "name", "hadoop", "lang", "java", "price", 300);
//
//        p_marko.addEdge("knows", vadas, "date", "20160110", "weight", 0.5);
//        p_marko.addEdge("knows", josh, "date", "20130220", "weight", 1.0);
//        p_marko.addEdge("created", s_marko, "date", "20171210", "weight", 0.4);
//        josh.addEdge("created", s_marko, "date", "20091111", "weight", 0.4);
//        josh.addEdge("created", ripple, "date", "20171210", "weight", 1.0);
//        peter.addEdge("created", s_marko, "date", "20170324", "weight", 0.2);

        graph.tx().commit();
    }
}
