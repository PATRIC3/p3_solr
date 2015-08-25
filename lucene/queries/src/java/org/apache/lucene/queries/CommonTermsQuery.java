package org.apache.lucene.queries;

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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.ToStringUtils;

/**
 * A query that executes high-frequency terms in a optional sub-query to prevent
 * slow queries due to "common" terms like stopwords. This query
 * builds 2 queries off the {@link #add(Term) added} terms: low-frequency
 * terms are added to a required boolean clause and high-frequency terms are
 * added to an optional boolean clause. The optional clause is only executed if
 * the required "low-frequency" clause matches. Scores produced by this query
 * will be slightly different than plain {@link BooleanQuery} scorer mainly due to
 * differences in the {@link Similarity#coord(int,int) number of leaf queries}
 * in the required boolean clause. In most cases, high-frequency terms are
 * unlikely to significantly contribute to the document score unless at least
 * one of the low-frequency terms are matched.  This query can improve
 * query execution times significantly if applicable.
 * <p>
 * {@link CommonTermsQuery} has several advantages over stopword filtering at
 * index or query time since a term can be "classified" based on the actual
 * document frequency in the index and can prevent slow queries even across
 * domains without specialized stopword files.
 * </p>
 * <p>
 * <b>Note:</b> if the query only contains high-frequency terms the query is
 * rewritten into a plain conjunction query ie. all high-frequency terms need to
 * match in order to match a document.
 * </p>
 */
public class CommonTermsQuery extends Query {
  /*
   * TODO maybe it would make sense to abstract this even further and allow to
   * rewrite to dismax rather than boolean. Yet, this can already be subclassed
   * to do so.
   */
  protected final List<Term> terms = new ArrayList<>();
  protected final boolean disableCoord;
  protected final float maxTermFrequency;
  protected final Occur lowFreqOccur;
  protected final Occur highFreqOccur;
  protected float lowFreqBoost = 1.0f;
  protected float highFreqBoost = 1.0f;
  protected float lowFreqMinNrShouldMatch = 0;
  protected float highFreqMinNrShouldMatch = 0;
  
  /**
   * Creates a new {@link CommonTermsQuery}
   * 
   * @param highFreqOccur
   *          {@link Occur} used for high frequency terms
   * @param lowFreqOccur
   *          {@link Occur} used for low frequency terms
   * @param maxTermFrequency
   *          a value in [0..1) (or absolute number &gt;=1) representing the
   *          maximum threshold of a terms document frequency to be considered a
   *          low frequency term.
   * @throws IllegalArgumentException
   *           if {@link Occur#MUST_NOT} is pass as lowFreqOccur or
   *           highFreqOccur
   */
  public CommonTermsQuery(Occur highFreqOccur, Occur lowFreqOccur,
      float maxTermFrequency) {
    this(highFreqOccur, lowFreqOccur, maxTermFrequency, false);
  }
  
  /**
   * Creates a new {@link CommonTermsQuery}
   * 
   * @param highFreqOccur
   *          {@link Occur} used for high frequency terms
   * @param lowFreqOccur
   *          {@link Occur} used for low frequency terms
   * @param maxTermFrequency
   *          a value in [0..1) (or absolute number &gt;=1) representing the
   *          maximum threshold of a terms document frequency to be considered a
   *          low frequency term.
   * @param disableCoord
   *          disables {@link Similarity#coord(int,int)} in scoring for the low
   *          / high frequency sub-queries
   * @throws IllegalArgumentException
   *           if {@link Occur#MUST_NOT} is pass as lowFreqOccur or
   *           highFreqOccur
   */
  public CommonTermsQuery(Occur highFreqOccur, Occur lowFreqOccur,
      float maxTermFrequency, boolean disableCoord) {
    if (highFreqOccur == Occur.MUST_NOT) {
      throw new IllegalArgumentException(
          "highFreqOccur should be MUST or SHOULD but was MUST_NOT");
    }
    if (lowFreqOccur == Occur.MUST_NOT) {
      throw new IllegalArgumentException(
          "lowFreqOccur should be MUST or SHOULD but was MUST_NOT");
    }
    this.disableCoord = disableCoord;
    this.highFreqOccur = highFreqOccur;
    this.lowFreqOccur = lowFreqOccur;
    this.maxTermFrequency = maxTermFrequency;
  }
  
  /**
   * Adds a term to the {@link CommonTermsQuery}
   * 
   * @param term
   *          the term to add
   */
  public void add(Term term) {
    if (term == null) {
      throw new IllegalArgumentException("Term must not be null");
    }
    this.terms.add(term);
  }
  
  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (this.terms.isEmpty()) {
      return new MatchNoDocsQuery();
    } else if (this.terms.size() == 1) {
      final Query tq = newTermQuery(this.terms.get(0), null);
      tq.setBoost(getBoost());
      return tq;
    }
    final List<LeafReaderContext> leaves = reader.leaves();
    final int maxDoc = reader.maxDoc();
    final TermContext[] contextArray = new TermContext[terms.size()];
    final Term[] queryTerms = this.terms.toArray(new Term[0]);
    collectTermContext(reader, leaves, contextArray, queryTerms);
    return buildQuery(maxDoc, contextArray, queryTerms);
  }
  
  protected int calcLowFreqMinimumNumberShouldMatch(int numOptional) {
    return minNrShouldMatch(lowFreqMinNrShouldMatch, numOptional);
  }
  
  protected int calcHighFreqMinimumNumberShouldMatch(int numOptional) {
    return minNrShouldMatch(highFreqMinNrShouldMatch, numOptional);
  }
  
  private final int minNrShouldMatch(float minNrShouldMatch, int numOptional) {
    if (minNrShouldMatch >= 1.0f || minNrShouldMatch == 0.0f) {
      return (int) minNrShouldMatch;
    }
    return Math.round(minNrShouldMatch * numOptional);
  }
  
  protected Query buildQuery(final int maxDoc,
      final TermContext[] contextArray, final Term[] queryTerms) {
    List<Query> lowFreqQueries = new ArrayList<>();
    List<Query> highFreqQueries = new ArrayList<>();
    for (int i = 0; i < queryTerms.length; i++) {
      TermContext termContext = contextArray[i];
      if (termContext == null) {
        lowFreqQueries.add(newTermQuery(queryTerms[i], null));
      } else {
        if ((maxTermFrequency >= 1f && termContext.docFreq() > maxTermFrequency)
            || (termContext.docFreq() > (int) Math.ceil(maxTermFrequency
                * (float) maxDoc))) {
          highFreqQueries
              .add(newTermQuery(queryTerms[i], termContext));
        } else {
          lowFreqQueries.add(newTermQuery(queryTerms[i], termContext));
        }
      }
    }
    final int numLowFreqClauses = lowFreqQueries.size();
    final int numHighFreqClauses = highFreqQueries.size();
    Occur lowFreqOccur = this.lowFreqOccur;
    Occur highFreqOccur = this.highFreqOccur;
    int lowFreqMinShouldMatch = 0;
    int highFreqMinShouldMatch = 0;
    if (lowFreqOccur == Occur.SHOULD && numLowFreqClauses > 0) {
      lowFreqMinShouldMatch = calcLowFreqMinimumNumberShouldMatch(numLowFreqClauses);
    }
    if (highFreqOccur == Occur.SHOULD && numHighFreqClauses > 0) {
      highFreqMinShouldMatch = calcHighFreqMinimumNumberShouldMatch(numHighFreqClauses);
    }
    if (lowFreqQueries.isEmpty()) {
      /*
       * if lowFreq is empty we rewrite the high freq terms in a conjunction to
       * prevent slow queries.
       */
      if (highFreqMinShouldMatch == 0 && highFreqOccur != Occur.MUST) {
        highFreqOccur = Occur.MUST;
      }
    }
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.setDisableCoord(true);

    if (lowFreqQueries.isEmpty() == false) {
      BooleanQuery.Builder lowFreq = new BooleanQuery.Builder();
      lowFreq.setDisableCoord(disableCoord);
      for (Query query : lowFreqQueries) {
        lowFreq.add(query, lowFreqOccur);
      }
      lowFreq.setMinimumNumberShouldMatch(lowFreqMinShouldMatch);
      Query lowFreqQuery = lowFreq.build();
      lowFreqQuery.setBoost(lowFreqBoost);
      builder.add(lowFreqQuery, Occur.MUST);
    }
    if (highFreqQueries.isEmpty() == false) {
      BooleanQuery.Builder highFreq = new BooleanQuery.Builder();
      highFreq.setDisableCoord(disableCoord);
      for (Query query : highFreqQueries) {
        highFreq.add(query, highFreqOccur);
      }
      highFreq.setMinimumNumberShouldMatch(highFreqMinShouldMatch);
      Query highFreqQuery = highFreq.build();
      highFreqQuery.setBoost(highFreqBoost);
      builder.add(highFreqQuery, Occur.SHOULD);
    }
    Query rewritten = builder.build();
    rewritten.setBoost(getBoost());
    return rewritten;
  }
  
  public void collectTermContext(IndexReader reader,
      List<LeafReaderContext> leaves, TermContext[] contextArray,
      Term[] queryTerms) throws IOException {
    TermsEnum termsEnum = null;
    for (LeafReaderContext context : leaves) {
      final Fields fields = context.reader().fields();
      for (int i = 0; i < queryTerms.length; i++) {
        Term term = queryTerms[i];
        TermContext termContext = contextArray[i];
        final Terms terms = fields.terms(term.field());
        if (terms == null) {
          // field does not exist
          continue;
        }
        termsEnum = terms.iterator();
        assert termsEnum != null;
        
        if (termsEnum == TermsEnum.EMPTY) continue;
        if (termsEnum.seekExact(term.bytes())) {
          if (termContext == null) {
            contextArray[i] = new TermContext(reader.getContext(),
                termsEnum.termState(), context.ord, termsEnum.docFreq(),
                termsEnum.totalTermFreq());
          } else {
            termContext.register(termsEnum.termState(), context.ord,
                termsEnum.docFreq(), termsEnum.totalTermFreq());
          }
          
        }
        
      }
    }
  }
  
  /**
   * Returns true iff {@link Similarity#coord(int,int)} is disabled in scoring
   * for the high and low frequency query instance. The top level query will
   * always disable coords.
   */
  public boolean isCoordDisabled() {
    return disableCoord;
  }
  
  /**
   * Specifies a minimum number of the low frequent optional BooleanClauses which must be
   * satisfied in order to produce a match on the low frequency terms query
   * part. This method accepts a float value in the range [0..1) as a fraction
   * of the actual query terms in the low frequent clause or a number
   * <tt>&gt;=1</tt> as an absolut number of clauses that need to match.
   * 
   * <p>
   * By default no optional clauses are necessary for a match (unless there are
   * no required clauses). If this method is used, then the specified number of
   * clauses is required.
   * </p>
   * 
   * @param min
   *          the number of optional clauses that must match
   */
  public void setLowFreqMinimumNumberShouldMatch(float min) {
    this.lowFreqMinNrShouldMatch = min;
  }
  
  /**
   * Gets the minimum number of the optional low frequent BooleanClauses which must be
   * satisfied.
   */
  public float getLowFreqMinimumNumberShouldMatch() {
    return lowFreqMinNrShouldMatch;
  }
  
  /**
   * Specifies a minimum number of the high frequent optional BooleanClauses which must be
   * satisfied in order to produce a match on the low frequency terms query
   * part. This method accepts a float value in the range [0..1) as a fraction
   * of the actual query terms in the low frequent clause or a number
   * <tt>&gt;=1</tt> as an absolut number of clauses that need to match.
   * 
   * <p>
   * By default no optional clauses are necessary for a match (unless there are
   * no required clauses). If this method is used, then the specified number of
   * clauses is required.
   * </p>
   * 
   * @param min
   *          the number of optional clauses that must match
   */
  public void setHighFreqMinimumNumberShouldMatch(float min) {
    this.highFreqMinNrShouldMatch = min;
  }
  
  /**
   * Gets the minimum number of the optional high frequent BooleanClauses which must be
   * satisfied.
   */
  public float getHighFreqMinimumNumberShouldMatch() {
    return highFreqMinNrShouldMatch;
  }
  
  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    boolean needParens = (getBoost() != 1.0)
        || (getLowFreqMinimumNumberShouldMatch() > 0);
    if (needParens) {
      buffer.append("(");
    }
    for (int i = 0; i < terms.size(); i++) {
      Term t = terms.get(i);
      buffer.append(newTermQuery(t, null).toString());
      
      if (i != terms.size() - 1) buffer.append(", ");
    }
    if (needParens) {
      buffer.append(")");
    }
    if (getLowFreqMinimumNumberShouldMatch() > 0 || getHighFreqMinimumNumberShouldMatch() > 0) {
      buffer.append('~');
      buffer.append("(");
      buffer.append(getLowFreqMinimumNumberShouldMatch());
      buffer.append(getHighFreqMinimumNumberShouldMatch());
      buffer.append(")");
    }
    if (getBoost() != 1.0f) {
      buffer.append(ToStringUtils.boost(getBoost()));
    }
    return buffer.toString();
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + (disableCoord ? 1231 : 1237);
    result = prime * result + Float.floatToIntBits(highFreqBoost);
    result = prime * result
        + ((highFreqOccur == null) ? 0 : highFreqOccur.hashCode());
    result = prime * result + Float.floatToIntBits(lowFreqBoost);
    result = prime * result
        + ((lowFreqOccur == null) ? 0 : lowFreqOccur.hashCode());
    result = prime * result + Float.floatToIntBits(maxTermFrequency);
    result = prime * result + Float.floatToIntBits(lowFreqMinNrShouldMatch);
    result = prime * result + Float.floatToIntBits(highFreqMinNrShouldMatch);
    result = prime * result + ((terms == null) ? 0 : terms.hashCode());
    return result;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    CommonTermsQuery other = (CommonTermsQuery) obj;
    if (disableCoord != other.disableCoord) return false;
    if (Float.floatToIntBits(highFreqBoost) != Float
        .floatToIntBits(other.highFreqBoost)) return false;
    if (highFreqOccur != other.highFreqOccur) return false;
    if (Float.floatToIntBits(lowFreqBoost) != Float
        .floatToIntBits(other.lowFreqBoost)) return false;
    if (lowFreqOccur != other.lowFreqOccur) return false;
    if (Float.floatToIntBits(maxTermFrequency) != Float
        .floatToIntBits(other.maxTermFrequency)) return false;
    if (lowFreqMinNrShouldMatch != other.lowFreqMinNrShouldMatch) return false;
    if (highFreqMinNrShouldMatch != other.highFreqMinNrShouldMatch) return false;
    if (terms == null) {
      if (other.terms != null) return false;
    } else if (!terms.equals(other.terms)) return false;
    return true;
  }

  /**
   * Builds a new TermQuery instance.
   * <p>This is intended for subclasses that wish to customize the generated queries.</p>
   * @param term term
   * @param context the TermContext to be used to create the low level term query. Can be <code>null</code>.
   * @return new TermQuery instance
   */
  protected Query newTermQuery(Term term, TermContext context) {
    return context == null ? new TermQuery(term) : new TermQuery(term, context);
  }
}
