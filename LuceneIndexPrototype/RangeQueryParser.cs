﻿//-----------------------------------------------------------------------
// <copyright file="RangeQueryParser.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Index;
using Lucene.Net.QueryParsers;
using Lucene.Net.Search;
using Version = Lucene.Net.Util.Version;

namespace LuceneIndexPrototype
{

    public class RangeQueryParser : QueryParser
    {
        public static readonly Regex NumerciRangeValue = new Regex(@"^[\w\d]x[-\w\d.]+$", RegexOptions.Compiled);

        private readonly Dictionary<string, HashSet<string>> untokenized = new Dictionary<string, HashSet<string>>();
        private readonly Dictionary<Tuple<string, string>, string> replacedTokens = new Dictionary<Tuple<string, string>, string>();

        public RangeQueryParser(Version matchVersion, string f, Analyzer a)
            : base(matchVersion, f, a)
        {
        }

        public string ReplaceToken(string fieldName, string replacement)
        {
            var tokenReplacement = Guid.NewGuid().ToString("n");

            replacedTokens[Tuple.Create(fieldName, tokenReplacement)] = replacement;

            return tokenReplacement;
        }

        protected override Query GetPrefixQuery(string field, string termStr)
        {
            var fieldQuery = GetFieldQuery(field, termStr);

            var tq = fieldQuery as TermQuery;
            if (tq != null)
                return NewPrefixQuery(tq.Term);

            throw new InvalidOperationException("When trying to parse prefix clause for field '" + field + "' with value '" +
                                                termStr + "', we got a non term query, can't proceed: " + fieldQuery.GetType().Name + " " + fieldQuery);
        }

        protected override Query GetWildcardQuery(string field, string termStr)
        {
            if (termStr == "*")
            {
                return field == "*" ?
                    NewMatchAllDocsQuery() :
                    NewWildcardQuery(new Term(field, termStr));
            }

            var fieldQuery = GetFieldQuery(field, termStr);

            var tq = fieldQuery as TermQuery;
            if (tq != null)
                return NewWildcardQuery(tq.Term);

            var fieldQueryTypeName = fieldQuery == null ? "null" : fieldQuery.GetType().Name;
            throw new InvalidOperationException("When trying to parse wildcard clause for field '" + field + "' with value '" +
                                                        termStr + "', we got a non term query, can't proceed: " + fieldQueryTypeName + " " + fieldQuery);
        }

        protected override Query GetFuzzyQuery(string field, string termStr, float minSimilarity)
        {
            var fieldQuery = GetFieldQuery(field, termStr);

            var tq = fieldQuery as TermQuery;
            if (tq != null)
                return NewFuzzyQuery(tq.Term, minSimilarity, FuzzyPrefixLength);

            throw new InvalidOperationException("When trying to parse fuzzy clause for field '" + field + "' with value '" +
                                                termStr + "', we got a non term query, can't proceed: " + fieldQuery.GetType().Name + " " + fieldQuery);

        }

        protected override Query GetFieldQuery(string field, string queryText)
        {
            string value;
            if (replacedTokens.TryGetValue(Tuple.Create(field, queryText), out value))
                return new TermQuery(new Term(field, value));

            HashSet<string> set;
            if (untokenized.TryGetValue(field, out set))
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
        protected override Query GetRangeQuery(string field, string lower, string upper, bool inclusive)
        {
            if (lower == "NULL" || lower == "*")
                lower = null;
            if (upper == "NULL" || upper == "*")
                upper = null;

            if ((lower == null || !NumerciRangeValue.IsMatch(lower)) && (upper == null || !NumerciRangeValue.IsMatch(upper)))
            {
                return NewRangeQuery(field, lower, upper, inclusive);
            }

            var from = RangeQueryParser.StringToNumber(lower);
            var to = RangeQueryParser.StringToNumber(upper);

            TypeCode numericType;

            if (from != null)
                numericType = Type.GetTypeCode(from.GetType());
            else if (to != null)
                numericType = Type.GetTypeCode(to.GetType());
            else
                numericType = TypeCode.Empty;

            switch (numericType)
            {
                case TypeCode.Int32:
                    {
                        return NumericRangeQuery.NewIntRange(field, (int)(from ?? Int32.MinValue), (int)(to ?? Int32.MaxValue), inclusive, inclusive);
                    }
                case TypeCode.Int64:
                    {
                        return NumericRangeQuery.NewLongRange(field, (long)(from ?? Int64.MinValue), (long)(to ?? Int64.MaxValue), inclusive, inclusive);
                    }
                case TypeCode.Double:
                    {
                        return NumericRangeQuery.NewDoubleRange(field, (double)(from ?? Double.MinValue), (double)(to ?? Double.MaxValue), inclusive, inclusive);
                    }
                case TypeCode.Single:
                    {
                        return NumericRangeQuery.NewFloatRange(field, (float)(from ?? Single.MinValue), (float)(to ?? Single.MaxValue), inclusive, inclusive);
                    }
                default:
                    {
                        return NewRangeQuery(field, lower, upper, inclusive);
                    }
            }
        }

        public void SetUntokenized(string field, string value)
        {
            HashSet<string> set;
            if (untokenized.TryGetValue(field, out set) == false)
            {
                untokenized[field] = set = new HashSet<string>();
            }
            set.Add(value);
        }

        /// <summary>
        /// Translate an indexable string to a number
        /// </summary>
        public static object StringToNumber(string number)
        {
            if (number == null)
                return null;

            if ("NULL".Equals(number, StringComparison.InvariantCultureIgnoreCase) ||
                "*".Equals(number, StringComparison.InvariantCultureIgnoreCase))
                return null;
            if (number.Length <= 2)
                throw new ArgumentException("String must be greater than 2 characters");
            var num = number.Substring(2);
            var prefix = number.Substring(0, 2);
            switch (prefix)
            {
                case "0x":
                    switch (num.Length)
                    {
                        case 8:
                            return int.Parse(num, NumberStyles.HexNumber);
                        case 16:
                            return long.Parse(num, NumberStyles.HexNumber);
                    }
                    break;
                case "Ix":
                    return int.Parse(num, CultureInfo.InvariantCulture);
                case "Lx":
                    return long.Parse(num, CultureInfo.InvariantCulture);
                case "Fx":
                    return float.Parse(num, CultureInfo.InvariantCulture);
                case "Dx":
                    return double.Parse(num, CultureInfo.InvariantCulture);
            }

            throw new ArgumentException(string.Format("Could not understand how to parse: '{0}'", number));

        }
    }
}
