using Cassandra;
using PolyPersist.Net.Common;
using PolyPersist.Net.Extensions;
using System.Collections;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq.Expressions;

namespace PolyPersist.Net.ColumnStore.Cassandra
{
    internal class Cassandra_Queryable<TRow> : IQueryableAsync<TRow>
        where TRow : IRow, new()
    {
        protected readonly Expression _expression;
        protected readonly Cassandra_QueryProvider<TRow> _provider;

        public Cassandra_Queryable(Cassandra_ColumnTable<TRow> table)
        {
            _provider = new Cassandra_QueryProvider<TRow>(table);
            _expression = Expression.Constant(this);
        }

        public Cassandra_Queryable(Cassandra_QueryProvider<TRow> provider, Expression expression)
        {
            _provider = provider;
            _expression = expression;
        }

        public Type ElementType => typeof(TRow);
        public Expression Expression => _expression;
        public IQueryProvider Provider => _provider;

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        public IEnumerator<TRow> GetEnumerator()
        {
            return _provider
                .Execute<IEnumerable<TRow>>(_expression)
                .GetEnumerator();
        }

        IAsyncEnumerator<TRow> IAsyncEnumerable<TRow>.GetAsyncEnumerator(CancellationToken cancellationToken) => GetAsyncEnumerator(cancellationToken);
        public async IAsyncEnumerator<TRow> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            await foreach (var item in _provider.ExecuteAsync(_expression).WithCancellation(cancellationToken))
            {
                yield return item;
            }
        }
    }

    internal class Cassandra_QueryProvider<TRow> : IQueryProvider
        where TRow : IRow, new()
    {
        private readonly Cassandra_ColumnTable<TRow> _table;

        public Cassandra_QueryProvider(Cassandra_ColumnTable<TRow> table)
        {
            _table = table;
        }

        public IQueryable CreateQuery(Expression expression)
        {
            return new Cassandra_Queryable<TRow>(this, expression);
        }

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            return (IQueryable<TElement>)new Cassandra_Queryable<TRow>(new Cassandra_QueryProvider<TRow>(_table), expression);
        }


        private (string Cql, Func<Row, CqlColumn[], TRow> Mapper) BuildQueryPlan(Expression expression)
        {
            var visitor = new Cassandra_ExpressionVisitor(_table._session.Keyspace, _table._tableName, _table._tableMeta);
            string whereClause = visitor.TranslateWhere(expression);
            string selectClause = visitor.TranslateSelect(expression) ?? "*";
            string orderByClause = visitor.TranslateOrderBy();
            string limitClause = visitor.TranslateLimit();
            string allowFiltering = visitor.IsAllowFiltering() ? " ALLOW FILTERING " : string.Empty;

            var cql = $"SELECT {selectClause} FROM {_table._tableName}{whereClause}{allowFiltering}{orderByClause}{limitClause};";

            var accessors = MetadataHelper
                .GetAccessors<TRow>()
                .ToDictionary(kvp => kvp.Key.ToLower(), kvp => kvp.Value);

            var usedAccessors = (visitor.SelectedFieldSet != null && visitor.SelectedFieldSet.Count > 0)
                ? accessors.Where(kvp => visitor.SelectedFieldSet.Contains(kvp.Key)).ToList()
                : accessors.ToList();

            Func<TRow> instanceFactory = () => new TRow();

            Func<Row, CqlColumn[], TRow> mapper = (row,columns) =>
            {
                var item = instanceFactory();
                var fieldIndexMap = new Dictionary<string, int>();
                for (int i = 0; i < columns.Length; i++)
                    fieldIndexMap[columns[i].Name.ToLower()] = i;

                foreach (var (name, accessor) in usedAccessors)
                {
                    if (fieldIndexMap.TryGetValue(name, out var fieldIndex) && accessor.Setter != null)
                    {
                        var getter = Cassandra_Mapper.BuildTypedGetter(accessor.Type);
                        var value = getter(row, fieldIndex);
                        accessor.Setter(item, value);
                    }
                }

                return item;
            };

            return (cql, mapper);
        }

        public object Execute(Expression expression)
        {
            var (cql, mapper) = BuildQueryPlan(expression);
            RowSet rs = _table._session.Execute(new SimpleStatement(cql));

            IEnumerable<TRow> StreamRows()
            {
                foreach (var row in rs)
                    yield return mapper(row, rs.Columns);
            }

            return StreamRows();
        }

        public async IAsyncEnumerable<TRow> ExecuteAsync(Expression expression)
        {
            var (cql, mapper) = BuildQueryPlan(expression);
            var statement = new SimpleStatement(cql).SetPageSize(5000);
            RowSet rs = await _table._session.ExecuteAsync(statement);

            do
            {
                foreach (var row in rs)
                    yield return mapper(row, rs.Columns);

                if (rs.PagingState != null)
                {
                    var pagedStatement = new SimpleStatement(cql)
                        .SetPageSize(5000)
                        .SetPagingState(rs.PagingState);
                    rs = await _table._session.ExecuteAsync(pagedStatement);
                }
                else
                {
                    break;
                }

            } while (true);
        }
        public TResult Execute<TResult>(Expression expression) => (TResult)Execute(expression);
    }

    internal class Cassandra_ExpressionVisitor : ExpressionVisitor
    {
        private string _keyspace;
        private string _tableName;
        private TableMetadata _tableMeta;
        private readonly List<string> _conditions = new();
        private string _orderBy = null;
        private int? _limit = null;
        private bool _allowFiltering = false;

        private List<string> _selectedFields;
        private HashSet<string> _selectedFieldSet;
        public HashSet<string> SelectedFieldSet =>
            _selectedFieldSet ??= _selectedFields != null
                ? new HashSet<string>(_selectedFields)
                : null;

        internal Cassandra_ExpressionVisitor(string keyspace, string tableName, TableMetadata tableMeta)
        {
            _keyspace = keyspace;
            _tableName = tableName;
            _tableMeta = tableMeta;
        }

        private static readonly ConcurrentDictionary<(string Keyspace, string Table), HashSet<string>> _filterableColumnsCache = new();
        

        private bool IsColumnFilterable(string columnName)
        {
            var key = (_keyspace, _tableName);

            if (!_filterableColumnsCache.TryGetValue(key, out var filterable))
            {
                filterable = new HashSet<string>(
                    _tableMeta.PartitionKeys.Select(k => k.Name)
                    .Concat(
                        _tableMeta.Indexes.Values
                        .Where(i => !string.IsNullOrEmpty(i.Target))
                        .Select(i => i.Target.Split(' ').Last().Trim('"'))
                    ),
                    StringComparer.OrdinalIgnoreCase);

                _filterableColumnsCache[key] = filterable;
            }

            return filterable.Contains(columnName);
        }

        internal bool IsAllowFiltering() => _allowFiltering;


        public string TranslateWhere(Expression expression)
        {
            Visit(expression);
            return _conditions.Count > 0 ? " WHERE " + string.Join(" ", _conditions) : "";
        }

        public string TranslateSelect(Expression expression)
        {
            _selectedFields = new();
            Visit(expression);
            return _selectedFields?.Count > 0 ? string.Join(", ", _selectedFields) : null;
        }

        public string TranslateOrderBy() => _orderBy != null ? $" ORDER BY {_orderBy}" : "";
        public string TranslateLimit() => _limit.HasValue ? $" LIMIT {_limit}" : "";

        protected override Expression VisitBinary(BinaryExpression node)
        {
            if (node.NodeType == ExpressionType.AndAlso || node.NodeType == ExpressionType.OrElse)
            {
                Visit(node.Left);
                _conditions.Add(node.NodeType == ExpressionType.AndAlso ? "AND" : "OR");
                Visit(node.Right);
                return node;
            }

            string left;
            if (node.Left is MemberExpression leftMember)
            {
                if (leftMember.Member.Name == "Length" && leftMember.Expression is MemberExpression strMember)
                {
                    left = $"length({strMember.Member.Name.ToLower()})";
                }
                else
                {
                    left = leftMember.Member.Name.ToLower();

                    if (IsColumnFilterable(left) == false)
                    {
                        _allowFiltering = true;
                        Trace.TraceWarning( $" Member name {_tableName}.{leftMember.Member.Name} is used in condition, but it is not indexed field. Can be very slow!");
                    }
                }
            }
            else
            {
                throw new NotSupportedException("Left side of binary expression must be a member expression");
            }

            object right = TryEvaluateConstans(node.Right);
            string opSymbol = node.NodeType switch
            {
                ExpressionType.Equal => "=",
                ExpressionType.NotEqual => "!=",
                ExpressionType.GreaterThan => ">",
                ExpressionType.GreaterThanOrEqual => ">=",
                ExpressionType.LessThan => "<",
                ExpressionType.LessThanOrEqual => "<=",
                _ => throw new NotSupportedException($"Operator '{node.NodeType}' not supported")
            };

            string valStr = right switch
            {
                string s => $"'{s}'",
                DateTime dt => $"'{dt:yyyy-MM-dd HH:mm:ss}'",
                DateOnly d => $"'{d:yyyy-MM-dd}'",
                TimeOnly t => ((long)(t.ToTimeSpan().TotalMilliseconds * 1_000_000)).ToString(),
                Guid g => $"'{g}'",
                bool b => b.ToString().ToLower(),
                _ => right.ToString()
            };

            _conditions.Add($"{left} {opSymbol} {valStr}");
            return node;
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            if (node.NodeType == ExpressionType.Not)
            {
                _conditions.Add("NOT (");
                Visit(node.Operand);
                _conditions.Add(")");
                return node;
            }
            return base.VisitUnary(node);
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.Name == "Count" && node.Arguments.Count == 1)
            {
                _selectedFields = new List<string> { "COUNT(*)" };
                Visit(node.Arguments[0]);
                return node;
            }

            if (node.Method.Name == "Where" && node.Arguments.Count == 2)
            {
                var lambda = (LambdaExpression)StripQuotes(node.Arguments[1]);
                Visit(lambda.Body);
                Visit(node.Arguments[0]);
                return node;
            }

            if (node.Method.Name == "ToLower" && node.Object is MemberExpression toLowerMember)
            {
                string column = toLowerMember.Member.Name.ToLower();
                _conditions.Add($"lower({column})");
                return node;
            }

            if (node.Method.Name == "ToUpper" && node.Object is MemberExpression toUpperMember)
            {
                string column = toUpperMember.Member.Name.ToLower();
                _conditions.Add($"upper({column})");
                return node;
            }

            if (node.Method.Name == "Trim" && node.Object is MemberExpression trimMember)
            {
                string column = trimMember.Member.Name.ToLower();
                _conditions.Add($"trim({column})");
                return node;
            }

            if (node.Method.Name == "Contains")
            {
                if (node.Object is MemberExpression member)
                {
                    // string.Contains
                    string column = member.Member.Name.ToLower();
                    object value = TryEvaluateConstans(node.Arguments[0]);
                    string valStr = $"%{value}%";
                    _conditions.Add($"{column} LIKE '{valStr}'");
                    return node;
                }
                else if (node.Object == null && node.Arguments.Count == 2)
                {
                    // collection.Contains(x.prop)
                    var values = TryEvaluateConstans(node.Arguments[0]) as IEnumerable;
                    var memberExpr = node.Arguments[1] as MemberExpression;
                    if (values != null && memberExpr != null)
                    {
                        string column = memberExpr.Member.Name.ToLower();
                        var inList = new List<string>();
                        foreach (var val in values)
                        {
                            inList.Add(val is string s ? $"'{s}'" : val.ToString());
                        }
                        _conditions.Add($"{column} IN ({string.Join(", ", inList)})");
                        return node;
                    }
                }
            }

            if (node.Method.Name == "StartsWith" && node.Object is MemberExpression memberSw)
            {
                string column = memberSw.Member.Name.ToLower();
                object value = TryEvaluateConstans(node.Arguments[0]);
                string valStr = $"{value}%";
                _conditions.Add($"{column} LIKE '{valStr}'");
                return node;
            }

            if (node.Method.Name == "EndsWith" && node.Object is MemberExpression memberEw)
            {
                string column = memberEw.Member.Name.ToLower();
                object value = TryEvaluateConstans(node.Arguments[0]);
                string valStr = $"%{value}";
                _conditions.Add($"{column} LIKE '{valStr}'");
                return node;
            }

            if (node.Method.Name == "Take" && node.Arguments.Count == 2)
            {
                var count = (int)TryEvaluateConstans(node.Arguments[1]);
                _limit = count;
                Visit(node.Arguments[0]);
                return node;
            }

            if ((node.Method.Name == "OrderBy" || node.Method.Name == "OrderByDescending") && node.Arguments.Count == 2)
            {
                var lambda = (LambdaExpression)StripQuotes(node.Arguments[1]);
                if (lambda.Body is MemberExpression memberExpr)
                {
                    _orderBy = $"{memberExpr.Member.Name.ToLower()} {(node.Method.Name == "OrderByDescending" ? "DESC" : "ASC")}";
                }
                Visit(node.Arguments[0]);
                return node;
            }

            throw new NotSupportedException($"The method '{node.Method.Name}' is not supported by Cassandra LINQ provider.");
        }


        protected override Expression VisitMemberInit(MemberInitExpression node)
        {
            foreach (var binding in node.Bindings)
            {
                if (binding is MemberAssignment assignment && assignment.Expression is MemberExpression memberExpr)
                {
                    _selectedFields.Add(memberExpr.Member.Name.ToLower());
                }
            }
            return node;
        }

        protected override Expression VisitNew(NewExpression node)
        {
            foreach (var arg in node.Arguments)
            {
                if (arg is MemberExpression memberExpr)
                {
                    _selectedFields.Add(memberExpr.Member.Name.ToLower());
                }
            }
            return node;
        }

        private static Expression StripQuotes(Expression e)
        {
            while (e.NodeType == ExpressionType.Quote)
            {
                e = ((UnaryExpression)e).Operand;
            }
            return e;
        }

        private static object TryEvaluateConstans(Expression expr)
        {
            return expr switch
            {
                ConstantExpression c => c.Value,
                MemberExpression m => Expression.Lambda(m).Compile().DynamicInvoke(),
                _ => throw new NotSupportedException("Expression type not supported")
            };
        }
    }
}
