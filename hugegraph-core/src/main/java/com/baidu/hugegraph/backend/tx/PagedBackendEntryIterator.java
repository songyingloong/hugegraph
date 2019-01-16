/*
 * Copyright (C) 2019 Baidu, Inc. All Rights Reserved.
 */

package com.baidu.hugegraph.backend.tx;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.iterator.Metadatable;
import com.baidu.hugegraph.util.E;

public class PagedBackendEntryIterator implements Iterator<BackendEntry>,
                                                  Metadatable {

    private final List<Query> queries;
    // The size of each page fetched by the inner page
    private final Function<Query, Iterator<BackendEntry>> fetcher;
    // The number of records remaining to be fetched
    private long remaining;
    private int offset;
    private Iterator<BackendEntry> results;

    public PagedBackendEntryIterator(List<Query> queries, String page,
                                     long limit,
                                     Function<Query, Iterator<BackendEntry>> fetcher) {
        E.checkNotNull(page, "page");
        this.queries = queries;
        this.fetcher = fetcher;
        this.remaining = limit;
        this.offset = 0;
        this.results = null;

        this.parsePageState(page);
    }

    private void parsePageState(String pageInfo) {
        if (pageInfo.isEmpty()) {
            for (int i = 0; i < this.queries.size(); i++) {
                Query query = this.queries.get(i);
                query.resetPage();
            }
        } else {
            PageState pageState = PageState.fromString(pageInfo);
            this.offset = pageState.offset();
            E.checkState(this.offset < this.queries.size(),
                         "Invalid page '%s' with an offset '%s' exceeds " +
                         "the size of Queries", pageInfo, this.offset);
            for (int i = offset; i < this.queries.size(); i++) {
                Query query = this.queries.get(i);
                if (i == offset) {
                    query.page(pageState.page());
                } else {
                    query.resetPage();
                }
            }
        }
    }

    @Override
    public boolean hasNext() {
        if (this.results != null && this.results.hasNext()) {
            return true;
        }

        if (this.remaining <= 0 || this.offset >= this.queries.size()) {
            return false;
        }

        if (this.results == null) {
            this.fetch();
        } else {
            if (++this.offset < this.queries.size()) {
                this.fetch();
            }
        }
        return this.results.hasNext();
    }

    private void fetch() {
        Query query = this.queries.get(this.offset);
        this.results = this.fetcher.apply(query);
        if (this.results.hasNext()) {
            return;
        }
        if (++this.offset < this.queries.size()) {
            this.fetch();
        }
    }

    @Override
    public BackendEntry next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }
        BackendEntry entry = this.results.next();
        this.remaining--;
        return entry;
    }

    @Override
    public Object metadata(String meta, Object... args) {
        if ("page".equals(meta)) {
            Object page = ((Metadatable) this.results).metadata(meta, args);
            PageState pageState = new PageState(this.offset, (String) page);
            return pageState.toString();
        }
        throw new NotSupportException("Invalid meta '%s'", meta);
    }
}
