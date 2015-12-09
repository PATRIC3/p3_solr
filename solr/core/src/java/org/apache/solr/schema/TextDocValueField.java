/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.schema;

import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.SortedSetFieldSource;
import org.apache.lucene.search.*;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.uninverting.UninvertingReader.Type;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.QueryBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.Sorting;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.io.IOException;

/** <code>TextField</code> is the basic type for configurable text analysis.
 * Analyzers for field types using this implementation should be defined in the schema.
 *
 */
public class TextDocValueField extends TextField {

  @Override
  public List<IndexableField> createFields(SchemaField field, Object value, float boost) {
    if (field.hasDocValues()) {
      List<IndexableField> fields = new ArrayList<>();
      fields.add(createField(field, value, boost));
      final BytesRef bytes = new BytesRef(value.toString());
      if (field.multiValued()) {
        fields.add(new SortedSetDocValuesField(field.getName(), bytes));
      } else {
        fields.add(new SortedDocValuesField(field.getName(), bytes));
      }
      return fields;
    } else {
//      return Collections.singletonList(createField(field, value, boost));
      return super.createFields(field, value, boost);
    }
  }

  @Override
  public void checkSchemaField(final SchemaField field) {
    // do nothing
  }

  @Override
  public boolean multiValuedFieldCache() {
    return false;
  }
}
