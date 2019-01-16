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

package com.baidu.hugegraph.backend.tx;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.iterator.Metadatable;
import com.baidu.hugegraph.util.E;

/**
 * 这个可能得换成迭代BackendEntry的结构
 */
public class PagedIdsIterator implements Iterator<Set<Id>>, Metadatable {

    private final IdHolderChain chain;
    // The size of each page fetched by the inner page
    private final int pageSize;
    // The number of records remaining to be fetched
    private long remaining;
    private int offset;
    private PagedIdHolder idsHolder;
    private Set<Id> results;

    public PagedIdsIterator(IdHolderChain chain, String page,
                            int pageSize, long limit) {
        E.checkArgument(chain.paged(), "IdHolderChain must be paged");
        E.checkNotNull(page, "page");
        this.chain = chain;
        this.pageSize = pageSize;
        this.remaining = limit;
        this.offset = 0;
        this.idsHolder = null;
        this.results = null;

        this.parsePageState(page);
    }

    private void parsePageState(String pageInfo) {
        if (pageInfo.isEmpty()) {
            for (int i = 0; i < this.chain.idsHolders().size(); i++) {
                IdHolder holder = this.chain.idsHolders().get(i);
                PagedIdHolder pagedHolder = (PagedIdHolder) holder;
                pagedHolder.resetPage();
            }
        } else {
            PageState pageState = PageState.fromString(pageInfo);
            this.offset = pageState.offset();
            E.checkState(this.offset < this.chain.idsHolders().size(),
                         "Invalid page '%s' with an offset '%s' exceeds " +
                         "the size of IdHolderChain", pageInfo, this.offset);
            for (int i = offset; i < this.chain.idsHolders().size(); i++) {
                IdHolder holder = this.chain.idsHolders().get(i);
                PagedIdHolder pagedHolder = (PagedIdHolder) holder;
                if (i == offset) {
                    pagedHolder.page(pageState.page());
                } else {
                    pagedHolder.resetPage();
                }
            }
        }
    }

    @Override
    public boolean hasNext() {
        if (this.results != null) {
            return true;
        }

        if (this.remaining <= 0 || this.offset >= this.chain.size()) {
            return false;
        }

        if (this.idsHolder == null || this.results == null) {
            this.fetch();
            return this.hasNext();
        }
        return this.results != null;
    }

    private void fetch() {
        this.idsHolder = (PagedIdHolder) this.chain.idsHolders()
                                                   .get(this.offset);
        if (this.remaining < this.pageSize) {
            this.idsHolder.limit((int) this.remaining);
        } else {
            this.idsHolder.limit(this.pageSize);
        }

        this.results = this.idsHolder.fetchFromBackend();

        if (this.results != null) {
            this.remaining -= this.results.size();
            this.makeUpOnePageIfNeed();
        } else {
            this.offset++;
        }
    }

    /**
     * If the result is not enough pagesize, should try to make up a page.
     */
    private void makeUpOnePageIfNeed() {
        if (this.results.size() < this.pageSize && this.remaining > 0) {
            if (++this.offset >= this.chain.size()) {
                return;
            }
            int lackCount = this.pageSize - this.results.size();
            this.idsHolder = (PagedIdHolder) this.chain.idsHolders()
                                                       .get(this.offset);
            this.idsHolder.limit(lackCount);
            Set<Id> compensated = this.idsHolder.fetchFromBackend();
            if (compensated != null) {
                this.remaining -= compensated.size();
                this.results.addAll(compensated);
            }
            this.makeUpOnePageIfNeed();
        }
    }

    @Override
    public Set<Id> next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }
        Set<Id> results = this.results;
        this.results = null;
        return results;
    }

    @Override
    public Object metadata(String meta, Object... args) {
        if ("page".equals(meta)) {
            String page = (String) this.idsHolder.metadata(meta, args);
            PageState pageState = new PageState(this.offset,
                                                               page);
            return pageState.toString();
        }
        throw new NotSupportException("Invalid meta '%s'", meta);
    }
}
