using System.Collections;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using Google.Cloud.BigQuery.V2;

namespace PolyPersist.Net.AnalyticalStore.BigQuery
{
    /// <summary>
    /// IQueryable over a BigQuery fact table. Enumeration (ToList / foreach) and terminal operators
    /// (Count / Sum / First / ...) are routed to <see cref="BigQuery_QueryProvider"/>, which
    /// translates the expression tree to GoogleSQL and executes it server-side.
    /// </summary>
    internal sealed class BigQuery_Queryable<T> : IOrderedQueryable<T>
    {
        private readonly BigQuery_QueryProvider _provider;

        public BigQuery_Queryable(BigQuery_QueryProvider provider)
        {
            _provider = provider;
            Expression = Expression.Constant(this);
        }

        public BigQuery_Queryable(BigQuery_QueryProvider provider, Expression expression)
        {
            _provider = provider;
            Expression = expression;
        }

        public Type ElementType => typeof(T);
        public Expression Expression { get; }
        public IQueryProvider Provider => _provider;

        public IEnumerator<T> GetEnumerator() => _provider.ExecuteSequence<T>(Expression).GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>
    /// Minimal LINQ-to-GoogleSQL provider covering the analytical query surface: Where, Select
    /// (row-column projection and grouped aggregate projection), GroupBy, OrderBy(Descending),
    /// Distinct, Take, and the terminals Count/Any/Sum/Min/Max/Average/First. Filtering and
    /// aggregation run inside BigQuery; only results are materialized.
    /// </summary>
    internal sealed class BigQuery_QueryProvider : IQueryProvider
    {
        private readonly BigQueryClient _client;
        private readonly string _tableRef;
        private readonly PropertyInfo[] _props;
        private readonly Type _recordType;

        public BigQuery_QueryProvider(BigQueryClient client, string tableRef, PropertyInfo[] props)
        {
            _client = client;
            _tableRef = tableRef;
            _props = props;
            _recordType = props.Length > 0 ? props[0].DeclaringType! : typeof(object);
        }

        // ---- IQueryProvider ----

        public IQueryable CreateQuery(Expression expression)
        {
            Type elem = _ElementType(expression.Type);
            var qt = typeof(BigQuery_Queryable<>).MakeGenericType(elem);
            return (IQueryable)Activator.CreateInstance(qt, this, expression)!;
        }

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
            => new BigQuery_Queryable<TElement>(this, expression);

        public object Execute(Expression expression) => ExecuteTerminal(expression, typeof(object))!;
        public TResult Execute<TResult>(Expression expression) => (TResult)ExecuteTerminal(expression, typeof(TResult))!;

        // ---- sequence (ToList / foreach) ----

        public IEnumerable<T> ExecuteSequence<T>(Expression expression)
        {
            var m = new Model();
            ProcessChain(expression, m);

            string where = WhereClause(m);
            string order = OrderClause(m);
            var result = new List<T>();

            if (m.GroupProjection != null)
            {
                var ne = m.GroupProjection;
                _groupKeyForProjection = m.GroupKeyCol;
                var selects = new List<string>();
                var aliases = new List<string>();
                for (int i = 0; i < ne.Arguments.Count; i++)
                {
                    string alias = ne.Members![i].Name;
                    // Backtick-quote the alias: an anonymous-type member may collide with a GoogleSQL
                    // reserved word (e.g. "Rows"). The row is still read back by the bare name.
                    selects.Add(TranslateGroupArg(ne.Arguments[i]) + " AS `" + alias + "`");
                    aliases.Add(alias);
                }
                string sql = $"SELECT {string.Join(", ", selects)} FROM {_tableRef} {where} GROUP BY {m.GroupKeyCol}".Trim();
                var ctorParams = ne.Constructor!.GetParameters();
                foreach (var row in Run(sql, m))
                {
                    var args = new object?[aliases.Count];
                    for (int i = 0; i < aliases.Count; i++)
                        args[i] = ConvertValue(row[aliases[i]], ctorParams[i].ParameterType);
                    result.Add((T)ne.Constructor.Invoke(args));
                }
            }
            else if (m.ScalarProjectionCol != null)
            {
                string distinct = m.Distinct ? "DISTINCT " : "";
                string sql = $"SELECT {distinct}{m.ScalarProjectionCol} AS v FROM {_tableRef} {where} {order}".Trim();
                foreach (var row in Run(sql, m))
                    result.Add((T)ConvertValue(row["v"], typeof(T))!);
            }
            else
            {
                string distinct = m.Distinct ? "DISTINCT " : "";
                string limit = m.Limit.HasValue ? $"LIMIT {m.Limit.Value}" : "";
                string sql = $"SELECT {distinct}* FROM {_tableRef} {where} {order} {limit}".Trim();
                foreach (var row in Run(sql, m))
                    result.Add((T)MaterializeRecord(row));
            }

            return result;
        }

        // ---- terminal operators ----

        private object? ExecuteTerminal(Expression expression, Type resultType)
        {
            var call = (MethodCallExpression)expression;
            string op = call.Method.Name;

            var m = new Model();
            ProcessChain(call.Arguments[0], m);
            string where = WhereClause(m);

            switch (op)
            {
                case "Count":
                case "LongCount":
                    return ConvertValue(Scalar($"SELECT COUNT(*) AS v FROM {_tableRef} {where}".Trim(), m), resultType);

                case "Any":
                    return Convert.ToInt64(Normalize(Scalar($"SELECT COUNT(*) AS v FROM {_tableRef} {where}".Trim(), m))) > 0;

                case "Sum":
                case "Min":
                case "Max":
                case "Average":
                {
                    string col = ColumnOf(GetLambda(call.Arguments[1]).Body);
                    string fn = op == "Sum" ? "SUM" : op == "Min" ? "MIN" : op == "Max" ? "MAX" : "AVG";
                    return ConvertValue(Scalar($"SELECT {fn}({col}) AS v FROM {_tableRef} {where}".Trim(), m), resultType);
                }

                case "First":
                case "FirstOrDefault":
                case "Single":
                case "SingleOrDefault":
                {
                    string order = OrderClause(m);
                    string sql = $"SELECT * FROM {_tableRef} {where} {order} LIMIT 1".Trim();
                    var row = Run(sql, m).FirstOrDefault();
                    if (row == null)
                    {
                        if (op.EndsWith("OrDefault")) return null;
                        throw new InvalidOperationException("Sequence contains no elements");
                    }
                    return MaterializeRecord(row);
                }

                default:
                    throw new NotSupportedException($"Query operator '{op}' is not supported by the BigQuery provider");
            }
        }

        // ---- expression -> model ----

        private void ProcessChain(Expression e, Model m)
        {
            if (e is MethodCallExpression call)
            {
                ProcessChain(call.Arguments[0], m);
                Apply(call, m);
            }
            // else: the root BigQuery_Queryable constant -> base SELECT * on the table
        }

        private void Apply(MethodCallExpression call, Model m)
        {
            switch (call.Method.Name)
            {
                case "Where":
                    m.Where.Add(Expr(GetLambda(call.Arguments[1]).Body, m));
                    break;
                case "GroupBy":
                    m.GroupKeyCol = ColumnOf(GetLambda(call.Arguments[1]).Body);
                    break;
                case "Select":
                    var sel = GetLambda(call.Arguments[1]);
                    if (m.GroupKeyCol != null)
                        m.GroupProjection = (NewExpression)sel.Body;
                    else
                        m.ScalarProjectionCol = ColumnOf(sel.Body);
                    break;
                case "OrderBy":
                    m.OrderByCol = ColumnOf(GetLambda(call.Arguments[1]).Body);
                    m.OrderDesc = false;
                    break;
                case "OrderByDescending":
                    m.OrderByCol = ColumnOf(GetLambda(call.Arguments[1]).Body);
                    m.OrderDesc = true;
                    break;
                case "Distinct":
                    m.Distinct = true;
                    break;
                case "Take":
                    m.Limit = Convert.ToInt32(Evaluate(call.Arguments[1]));
                    break;
                default:
                    throw new NotSupportedException($"Query operator '{call.Method.Name}' is not supported by the BigQuery provider");
            }
        }

        // Recursively render a predicate/scalar expression as GoogleSQL (columns, params, boolean/comparison ops).
        private string Expr(Expression e, Model m)
        {
            e = Unwrap(e);

            if (e is MemberExpression me && me.Expression is ParameterExpression)
                return me.Member.Name;

            if (e is BinaryExpression be)
            {
                string op = be.NodeType switch
                {
                    ExpressionType.AndAlso => "AND",
                    ExpressionType.OrElse => "OR",
                    ExpressionType.Equal => "=",
                    ExpressionType.NotEqual => "!=",
                    ExpressionType.GreaterThan => ">",
                    ExpressionType.GreaterThanOrEqual => ">=",
                    ExpressionType.LessThan => "<",
                    ExpressionType.LessThanOrEqual => "<=",
                    _ => throw new NotSupportedException($"Operator {be.NodeType} is not supported")
                };
                return $"({Expr(be.Left, m)} {op} {Expr(be.Right, m)})";
            }

            // Anything else is a value expression (constant or captured variable) -> parameter.
            object? val = Evaluate(e);
            string p = $"w{m.PIdx++}";
            m.Params.Add(MakeParam(p, val));
            return $"@{p}";
        }

        // g.Key -> group key column; g.Sum(x=>x.Col) -> SUM(Col); g.Count() -> COUNT(*).
        private string TranslateGroupArg(Expression arg)
        {
            arg = Unwrap(arg);

            if (arg is MemberExpression me && me.Member.Name == "Key")
                return _groupKeyForProjection!;

            if (arg is MethodCallExpression call)
            {
                string name = call.Method.Name;
                if (name == "Count")
                    return "COUNT(*)";
                if (name is "Sum" or "Min" or "Max" or "Average")
                {
                    string fn = name == "Sum" ? "SUM" : name == "Min" ? "MIN" : name == "Max" ? "MAX" : "AVG";
                    string col = ColumnOf(GetLambda(call.Arguments[1]).Body);
                    return $"{fn}({col})";
                }
            }

            throw new NotSupportedException($"Grouped projection expression '{arg}' is not supported");
        }

        // Set by ExecuteSequence before translating the grouped projection.
        private string? _groupKeyForProjection;

        // ---- execution ----

        private BigQueryResults Run(string sql, Model m)
            => _client.ExecuteQuery(sql, m.Params.Count > 0 ? m.Params : null);

        private object Scalar(string sql, Model m) => Run(sql, m).First()["v"];

        private object MaterializeRecord(BigQueryRow row)
        {
            var rec = Activator.CreateInstance(_recordType)!;
            foreach (var p in _props)
                p.SetValue(rec, ConvertValue(row[p.Name], p.PropertyType));
            return rec;
        }

        // ---- helpers ----

        private sealed class Model
        {
            public List<string> Where { get; } = new();
            public List<BigQueryParameter> Params { get; } = new();
            public int PIdx;
            public string? GroupKeyCol;
            public NewExpression? GroupProjection;
            public string? ScalarProjectionCol;
            public bool Distinct;
            public string? OrderByCol;
            public bool OrderDesc;
            public int? Limit;
        }

        private string WhereClause(Model m) => m.Where.Count > 0 ? "WHERE " + string.Join(" AND ", m.Where) : "";
        private string OrderClause(Model m) => m.OrderByCol != null ? $"ORDER BY {m.OrderByCol} {(m.OrderDesc ? "DESC" : "ASC")}" : "";

        private static LambdaExpression GetLambda(Expression e) => (LambdaExpression)(e is UnaryExpression u ? u.Operand : e);
        private static Expression Unwrap(Expression e) => e is UnaryExpression u && u.NodeType == ExpressionType.Convert ? u.Operand : e;

        private static string ColumnOf(Expression e)
        {
            e = Unwrap(e);
            if (e is MemberExpression me)
                return me.Member.Name;
            throw new NotSupportedException($"Expected a column reference but got '{e}'");
        }

        private static object? Evaluate(Expression e)
            => Expression.Lambda(Unwrap(e)).Compile().DynamicInvoke();

        private static Type _ElementType(Type type)
        {
            if (type.IsGenericType)
            {
                var def = type.GetGenericTypeDefinition();
                if (def == typeof(IQueryable<>) || def == typeof(IEnumerable<>) || def == typeof(IOrderedQueryable<>))
                    return type.GetGenericArguments()[0];
            }
            var iface = type.GetInterfaces().FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));
            return iface != null ? iface.GetGenericArguments()[0] : type;
        }

        private static object Normalize(object raw)
        {
            if (raw is BigQueryNumeric num) return decimal.Parse(num.ToString(), CultureInfo.InvariantCulture);
            if (raw is BigQueryBigNumeric big) return decimal.Parse(big.ToString(), CultureInfo.InvariantCulture);
            return raw;
        }

        private static object? ConvertValue(object? raw, Type target)
        {
            Type t = Nullable.GetUnderlyingType(target) ?? target;

            if (raw == null || raw is DBNull)
                return target.IsValueType && Nullable.GetUnderlyingType(target) == null ? Activator.CreateInstance(target) : null;

            raw = Normalize(raw);

            if (t.IsInstanceOfType(raw)) return raw;
            if (t.IsEnum) return Enum.ToObject(t, raw);
            return Convert.ChangeType(raw, t, CultureInfo.InvariantCulture);
        }

        private static BigQueryParameter MakeParam(string name, object? value)
        {
            switch (value)
            {
                case null: return new BigQueryParameter(name, BigQueryDbType.String, null);
                case string s: return new BigQueryParameter(name, BigQueryDbType.String, s);
                case bool b: return new BigQueryParameter(name, BigQueryDbType.Bool, b);
                case int or long or short or byte: return new BigQueryParameter(name, BigQueryDbType.Int64, Convert.ToInt64(value));
                case decimal d: return new BigQueryParameter(name, BigQueryDbType.Numeric, BigQueryNumeric.FromDecimal(d, LossOfPrecisionHandling.Truncate));
                case double or float: return new BigQueryParameter(name, BigQueryDbType.Float64, Convert.ToDouble(value));
                case DateTime dt: return new BigQueryParameter(name, BigQueryDbType.DateTime, dt);
                default: return new BigQueryParameter(name, BigQueryDbType.String, value.ToString());
            }
        }
    }
}
