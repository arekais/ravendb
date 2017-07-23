﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Client.Documents.Indexes;
using Version = Lucene.Net.Util.Version;

namespace Raven.Server.Documents.Queries.Parser.Lucene
{
    public class RangeQueryParser : global::Lucene.Net.QueryParsers.QueryParser
    {
        public static readonly Regex NumericRangeValue = new Regex(@"^[-\w\d.]+$", RegexOptions.Compiled);
        public static readonly Regex DateTimeValue = new Regex(@"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{7}Z?)", RegexOptions.Compiled);

        private readonly Dictionary<string, HashSet<string>> _untokenized = new Dictionary<string, HashSet<string>>();
        private readonly Dictionary<Tuple<string, string>, string> _replacedTokens = new Dictionary<Tuple<string, string>, string>();

        public RangeQueryParser(Version matchVersion, string f, Analyzer a)
            : base(matchVersion, f, a)
        {
        }

        public string ReplaceToken(string fieldName, string replacement)
        {
            var tokenReplacement = Guid.NewGuid().ToString("n");

            _replacedTokens[Tuple.Create(fieldName, tokenReplacement)] = replacement;

            return tokenReplacement;
        }

        public string ReplaceDateTimeTokensInMethod(string fieldName, string collection)
        {
            var searchMatches = DateTimeValue.Matches(collection);
            var queryStringBuilder = new StringBuilder(collection);
            for (var i = searchMatches.Count - 1; i >= 0; i--)
            {
                var searchMatch = searchMatches[i];
                var replaceToken = Guid.NewGuid().ToString("n");
                queryStringBuilder.Remove(searchMatch.Index, searchMatch.Length);
                queryStringBuilder.Insert(searchMatch.Index, replaceToken);
            }
            var tokenReplacement = queryStringBuilder.ToString();
            var keyOfTokenReplacement = queryStringBuilder.ToString(1, queryStringBuilder.Length - 2);
            _replacedTokens[Tuple.Create(fieldName, keyOfTokenReplacement)] = collection.Substring(1, collection.Length - 2);
            return tokenReplacement;
        }
        protected override global::Lucene.Net.Search.Query GetPrefixQuery(string field, string termStr)
        {
            var fieldQuery = GetFieldQuery(field, termStr);

            var tq = fieldQuery as TermQuery;
            if (tq == null)
            {
                var booleanQuery = new BooleanQuery
                {
                    {NewPrefixQuery(new Term(field, termStr)), Occur.SHOULD},
                    {NewPrefixQuery(new Term(field, termStr.ToLowerInvariant())), Occur.SHOULD}
                };
                return booleanQuery;
            }
            return NewPrefixQuery(tq.Term);
        }

        protected override global::Lucene.Net.Search.Query GetWildcardQuery(string field, string termStr)
        {
            if (termStr == "*")
            {
                return field == "*" ?
                    NewMatchAllDocsQuery() :
                    NewWildcardQuery(new Term(field, termStr));
            }

            var fieldQuery = GetFieldQuery(field, termStr);

            string analyzedTerm;
            var tq = fieldQuery as TermQuery;
            var pq = fieldQuery as PhraseQuery;
            if (tq != null)
            {
                analyzedTerm = tq.Term.Text;

                if (termStr.StartsWith("*") && analyzedTerm.StartsWith("*") == false)
                    analyzedTerm = "*" + analyzedTerm;

                if (termStr.EndsWith("*") && analyzedTerm.EndsWith("*") == false)
                    analyzedTerm += "*";
            }
            else if (pq != null)
            {
                // search ?,* in source not in target, add them per position. e.g: 
                // *foo* -> foo == *foo*
                // Bro?n -> bro n == "bro?n"

                var builder = new StringBuilder();
                foreach (var term in pq.GetTerms())
                {
                    if (builder.Length < termStr.Length)
                    {
                        var c = termStr[builder.Length];
                        if (c == '?' || c == '*')
                        {
                            builder.Append(c);
                        }
                    }
                    builder.Append(term.Text);
                }
                analyzedTerm = builder.ToString();
            }
            else
            {
                analyzedTerm = termStr;
            }

            return NewWildcardQuery(new Term(field, analyzedTerm));
        }

        protected override global::Lucene.Net.Search.Query GetFuzzyQuery(string field, string termStr, float minSimilarity)
        {
            var fieldQuery = GetFieldQuery(field, termStr);

            var tq = fieldQuery as TermQuery;
            return NewFuzzyQuery(tq != null ? tq.Term : new Term(field, termStr), minSimilarity, FuzzyPrefixLength);
        }

        protected override global::Lucene.Net.Search.Query GetFieldQuery(string field, string queryText)
        {
            string value;
            if (_replacedTokens.TryGetValue(Tuple.Create(field, queryText), out value))
                return new TermQuery(new Term(field, value));

            HashSet<string> set;
            if (_untokenized.TryGetValue(field, out set))
            {
                if (set.Contains(queryText))
                    return new TermQuery(new Term(field, queryText));
            }

            var fieldQuery = base.GetFieldQuery(field, queryText);
            if (fieldQuery is TermQuery
                && queryText.EndsWith("*")
                && !queryText.EndsWith(@"\*")
                && queryText.Contains(" "))
            {
                var analyzer = Analyzer;
                var tokenStream = analyzer.ReusableTokenStream(field, new StringReader(queryText.Substring(0, queryText.Length - 1)));
                var sb = new StringBuilder();
                while (tokenStream.IncrementToken())
                {
                    var attribute = (TermAttribute)tokenStream.GetAttribute<ITermAttribute>();
                    if (sb.Length != 0)
                        sb.Append(' ');
                    sb.Append(attribute.Term);
                }
                var prefix = new Term(field, sb.ToString());
                return new PrefixQuery(prefix);
            }
            return fieldQuery;
        }

        /// <summary>
        /// Detects numeric range terms and expands range expressions accordingly
        /// </summary>
        /// <param name="field"></param>
        /// <param name="lower"></param>
        /// <param name="upper"></param>
        /// <param name="inclusive"></param>
        /// <returns></returns>
        protected override global::Lucene.Net.Search.Query GetRangeQuery(string field, string lower, string upper, bool inclusive)
        {
            bool minInclusive = inclusive;
            bool maxInclusive = inclusive;
            if (lower == "NULL" || lower == "*")
            {
                lower = null;
                minInclusive = true;
            }
            if (upper == "NULL" || upper == "*")
            {
                upper = null;
                maxInclusive = true;
            }

            if ((lower == null || !NumericRangeValue.IsMatch(lower)) && (upper == null || !NumericRangeValue.IsMatch(upper)))
            {
                return NewRangeQuery(field, lower, upper, inclusive);
            }

            var rangeType = FieldUtil.GetRangeTypeFromFieldName(field);
            switch (rangeType)
            {
                case RangeType.Long:
                    var fromLong = NumberUtil.StringToLong(lower) ?? long.MinValue;
                    var toLong = NumberUtil.StringToLong(upper) ?? long.MaxValue;
                    return NumericRangeQuery.NewLongRange(field, fromLong, toLong, minInclusive, maxInclusive);
                case RangeType.Double:
                    var fromDouble = NumberUtil.StringToDouble(lower) ?? double.MinValue;
                    var toDouble = NumberUtil.StringToDouble(upper) ?? double.MaxValue;
                    return NumericRangeQuery.NewDoubleRange(field, fromDouble, toDouble, minInclusive, maxInclusive);
                default:
                    return NewRangeQuery(field, lower, upper, inclusive);
            }
        }

        public void SetUntokenized(string field, string value)
        {
            HashSet<string> set;
            if (_untokenized.TryGetValue(field, out set) == false)
            {
                _untokenized[field] = set = new HashSet<string>();
            }
            set.Add(value);
        }
    }
}