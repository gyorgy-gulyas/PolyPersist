using Cassandra;
using PolyPersist.Net.Common;
using System.Collections;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;

namespace PolyPersist.Net.ColumnStore.Cassandra
{
    internal class Cassandra_Queryable<TRow, TResult> : IOrderedQueryable<TResult>
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

        public Type ElementType => typeof(TResult);
        public Expression Expression => _expression;
        public IQueryProvider Provider => _provider;

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        public IEnumerator<TResult> GetEnumerator()
        {
            var result = Provider.Execute(Expression);
            var enumerable = (IEnumerable<TResult>)result;

            return enumerable.GetEnumerator();
        }

        //IAsyncEnumerator<TRow> IAsyncEnumerable<TRow>.GetAsyncEnumerator(CancellationToken cancellationToken) => GetAsyncEnumerator(cancellationToken);
        //public async IAsyncEnumerator<TRow> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        //{
        //    await foreach (var item in _provider.ExecuteAsync(_expression).WithCancellation(cancellationToken))
        //    {
        //        yield return item;
        //    }
        //}
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
            var elementType = expression.Type.GetGenericArguments().First();
            var queryableType = typeof(Cassandra_Queryable<,>).MakeGenericType(typeof(TRow), elementType);
            return (IQueryable)Activator.CreateInstance(queryableType, this, expression);
        }

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            return new Cassandra_Queryable<TRow, TElement>(this, expression);
        }

        private (string cql, Cassandra_ExpressionVisitor.ResultTypes resultType, Dictionary<string, string> projection, NewExpression projectionCtor) BuildQuery(Expression expression)
        {
            var visitor = new Cassandra_ExpressionVisitor(_table._session.Keyspace, _table._tableName, _table._tableMeta);
            visitor.Visit(expression);
            var (selectClause, resultType) = visitor.TranslateSelect();
            string distinctClause = visitor.TranslateDistinct();
            string whereClause = visitor.TranslateWhere();
            string orderByClause = visitor.TranslateOrderBy();
            string limitClause = visitor.TranslateLimit();
            string allowFiltering = visitor.IsAllowFiltering()
                ? " ALLOW FILTERING "
                : string.Empty;

            var cql = $"SELECT {distinctClause} {selectClause} FROM {_table._tableName}{whereClause}{allowFiltering}{orderByClause}{limitClause};";

            return (cql, resultType, visitor._projectionMap, visitor._projectionAnonymousCtor);
        }

        public object Execute(Expression expression)
        {
            var (cql, resulttype, projectionMap, _projectionAnonymousCtor) = BuildQuery(expression);
            RowSet rs = _table._session.Execute(new SimpleStatement(cql));

            if (expression.Type == typeof(int))
            {
                if (resulttype != Cassandra_ExpressionVisitor.ResultTypes.Count)
                    throw new NotSupportedException("Only count is supported when expected result is integer");

                return (int)rs.First().GetValue<long>("count");
            }
            else if (expression.Type == typeof(bool))
            {
            }
            else if (typeof(IEnumerable).IsAssignableFrom(expression.Type))
            {
                switch (resulttype)
                {
                    case Cassandra_ExpressionVisitor.ResultTypes.SelectRow:
                        {
                            var accessors = MetadataHelper.GetAccessors<TRow>().ToDictionary(kvp => kvp.Key.ToLower(), kvp => kvp.Value);

                            //  build index map, for reaching values in row based on index instad of name
                            var columns = rs.Columns;
                            var fieldIndexMap = new Dictionary<string, int>();
                            for (int i = 0; i < columns.Length; i++)
                                fieldIndexMap[columns[i].Name.ToLower()] = i;

                            var mappedAccessors = accessors
                                .Select(kvp => (
                                    FieldIndex: fieldIndexMap[kvp.Key],
                                    MemberAccessor: kvp.Value,
                                    CassandraValueGetter: Cassandra_Mapper.BuildTypedGetter(kvp.Value.Type)))
                                .ToList();

                            IEnumerable<TRow> StreamRows()
                            {
                                foreach (var row in rs)
                                {
                                    var item = new TRow();

                                    foreach (var (fieldIndex, memberAccessor, cassandraValueGetter) in mappedAccessors)
                                    {
                                        if (memberAccessor.Setter != null)
                                        {
                                            var value = cassandraValueGetter(row, fieldIndex); ;
                                            memberAccessor.Setter(item, value);
                                        }
                                    }

                                    yield return item;
                                }
                            }

                            return StreamRows();
                        }
                    case Cassandra_ExpressionVisitor.ResultTypes.ProjectionToClass:
                        {
                            Type resultItemType = expression.Type.GetGenericArguments().First();
                            var properties = resultItemType
                                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                                .Where(p => p.CanWrite)
                                .ToDictionary(p => p.Name.ToLower(), p => p);

                            var columns = rs.Columns;
                            var fieldIndexMap = new Dictionary<string, int>();
                            for (int i = 0; i < columns.Length; i++)
                                fieldIndexMap[columns[i].Name.ToLower()] = i;

                            var mappers = properties
                                .Select(kvp => (
                                    FieldIndex: fieldIndexMap[projectionMap[kvp.Key]],
                                    Property: kvp.Value,
                                    CassandraValueGetter: Cassandra_Mapper.BuildTypedGetter(kvp.Value.PropertyType)))
                                .ToList();

                            IEnumerable<object> StreamRows()
                            {
                                foreach (var row in rs)
                                {
                                    var item = Activator.CreateInstance(resultItemType);

                                    foreach (var (fieldIndex, property, cassandraGetter) in mappers)
                                    {
                                        var value = cassandraGetter(row, fieldIndex);
                                        property.SetValue(item, value);
                                    }

                                    yield return item;
                                }
                            }

                            // reflection-al hívjuk meg Enumerable.Cast<T>()
                            var castMethod = typeof(Enumerable)
                                .GetMethod(nameof(Enumerable.Cast), BindingFlags.Static | BindingFlags.Public)
                                .MakeGenericMethod(resultItemType);

                            var castedEnumerable = castMethod.Invoke(null, new object[] { StreamRows() });

                            return castedEnumerable;
                        }
                    case Cassandra_ExpressionVisitor.ResultTypes.ProjectionToAnonymous:
                        {
                            var columns = rs.Columns;
                            var fieldIndexMap = new Dictionary<string, int>();
                            for (int i = 0; i < columns.Length; i++)
                                fieldIndexMap[columns[i].Name.ToLower()] = i;

                            var mappers = _projectionAnonymousCtor
                                .Constructor
                                .GetParameters()
                                .Select( (parameter,index) => (
                                    FieldIndex: fieldIndexMap[projectionMap[parameter.Name]],
                                    Parameter: parameter,
                                    ParameterIndex: index,
                                    CassandraValueGetter: Cassandra_Mapper.BuildTypedGetter(parameter.ParameterType)))
                                .ToList();

                            IEnumerable<object> StreamRows()
                            {
                                foreach (var row in rs)
                                {
                                    var args = new object[mappers.Count];

                                    foreach (var (fieldIndex, param, paramIndex, cassandraGetter) in mappers)
                                    {
                                        var value = cassandraGetter(row, fieldIndex);
                                        args[paramIndex] = value;
                                    }

                                    yield return _projectionAnonymousCtor.Constructor.Invoke(args);
                                }
                            }

                            // reflection-al hívjuk meg Enumerable.Cast<T>()
                            Type resultItemType = expression.Type.GetGenericArguments().First();
                            var castMethod = typeof(Enumerable)
                                .GetMethod(nameof(Enumerable.Cast), BindingFlags.Static | BindingFlags.Public)
                                .MakeGenericMethod(resultItemType);

                            var castedEnumerable = castMethod.Invoke(null, new object[] { StreamRows() });

                            return castedEnumerable;
                        }
                    default:
                        throw new NotSupportedException("Only select row, or projection is alllowed with IEnumerable result");
                }
            }
            else
            {
            }

            return null;
        }
        public TResult Execute<TResult>(Expression expression) => (TResult)Execute(expression);

        //public async IAsyncEnumerable ExecuteAsync(Expression expression)
        //{
        //    var (cql, mapper) = BuildQueryPlan(expression);
        //    var statement = new SimpleStatement(cql).SetPageSize(5000);
        //    RowSet rs = await _table._session.ExecuteAsync(statement);

        //    do
        //    {
        //        foreach (var row in rs)
        //            yield return mapper(row, rs.Columns);

        //        if (rs.PagingState != null)
        //        {
        //            var pagedStatement = new SimpleStatement(cql)
        //                .SetPageSize(5000)
        //                .SetPagingState(rs.PagingState);
        //            rs = await _table._session.ExecuteAsync(pagedStatement);
        //        }
        //        else
        //        {
        //            break;
        //        }

        //    } while (true);
        //}
    }

    internal class Cassandra_ExpressionVisitor : ExpressionVisitor
    {
        private string _keyspace;
        private string _tableName;
        private TableMetadata _tableMeta;
        private readonly List<string> _conditions = new();
        private List<string> _orderBy = new();
        private int? _limit = null;
        private bool _allowFiltering = false;

        internal List<string> _selectedFields = new();
        internal Dictionary<string, string> _projectionMap = new();

        internal enum ResultTypes
        {
            Count,
            SelectRow,
            ProjectionToClass,
            ProjectionToAnonymous,
        }

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

        public string TranslateDistinct()
        {
            return _isDistinct
                ? " DISTINCT "
                : "";
        }

        public string TranslateWhere()
        {
            return _conditions.Count > 0
                ? " WHERE " + string.Join(" ", _conditions)
                : "";
        }

        public (string, ResultTypes) TranslateSelect()
        {
            if (_isCount)
            {
                return (" COUNT(*) ", ResultTypes.Count);
            }
            else if (_selectedFields?.Count > 0)
            {
                return (string.Join(", ", _selectedFields), _projectionAnonymousCtor != null
                    ? ResultTypes.ProjectionToAnonymous
                    : ResultTypes.ProjectionToClass);
            }
            else
                return ("*", ResultTypes.SelectRow);
        }

        public string TranslateOrderBy() => _orderBy.Count > 0
            ? $" ORDER BY {string.Join(", ", _orderBy)}"
            : "";

        public string TranslateLimit() => _limit.HasValue
            ? $" LIMIT {_limit}"
            : "";

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
            string memberName;
            if (node.Left is MemberExpression leftMember)
            {
                if (leftMember.Member.Name == "Length" && leftMember.Expression is MemberExpression strMember)
                {
                    left = $"length({strMember.Member.Name.ToLower()})";
                    memberName = strMember.Member.Name.ToLower();

                    _allowFiltering = true;
                    Trace.TraceWarning($" using 'length' on {_tableName}.{memberName}. Can be very slow!");
                }
                else
                {
                    left = leftMember.Member.Name.ToLower();
                    memberName = leftMember.Member.Name.ToLower();
                }
            }
            else if (node.Left is MethodCallExpression methodCall)
            {
                throw new NotSupportedException($"Unsupported method call on left side: {methodCall.Method.Name}");
            }
            else
            {
                throw new NotSupportedException($"Left side of binary expression must be a member, not a {node.Left.NodeType}");
            }

            if (IsColumnFilterable(memberName) == false && _allowFiltering == false)
            {
                _allowFiltering = true;
                Trace.TraceWarning($" Member name {_tableName}.{memberName} is used in condition, but it is not indexed field. Can be very slow!");
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

        protected override Expression VisitMember(MemberExpression node)
        {
            // bool property check: eg. x => x.IsActive
            if (node.Type == typeof(bool) && node.Expression is ParameterExpression)
            {
                _conditions.Add($"{node.Member.Name.ToLower()} = true");
                return node;
            }

            return base.VisitMember(node);
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            if (node.NodeType == ExpressionType.Not &&
                node.Operand is MemberExpression memberExpr &&
                memberExpr.Type == typeof(bool) &&
                memberExpr.Expression is ParameterExpression)
            {
                _conditions.Add($"{memberExpr.Member.Name.ToLower()} = false");
                return node;
            }
            return base.VisitUnary(node);
        }

        private bool _isCount = false;
        private bool _isDistinct = false;

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.Name == "Count" && node.Arguments.Count == 1)
            {
                _isCount = true;
                Visit(node.Arguments[0]);
                return node;
            }

            if (node.Method.Name == "Distinct" && node.Arguments.Count == 1)
            {
                _isDistinct = true;
                Visit(node.Arguments[0]);
                return node;
            }

            if (node.Method.Name == "Select" && node.Arguments.Count == 2)
            {
                _isCount = false;
                var lambda = (LambdaExpression)StripQuotes(node.Arguments[1]);

                if (lambda.Body is MemberInitExpression init)
                {
                    VisitMemberInit(init);
                }
                else if (lambda.Body is NewExpression newExpr)
                {
                    VisitNew(newExpr);
                }
                else
                {
                    throw new NotSupportedException( $"Unsupported Select projection expression: {node.NodeType}.");
                }

                Visit(node.Arguments[0]); // input sequence feldolgozása (pl. Where)
                return node;
            }

            if (node.Method.Name == "Where" && node.Arguments.Count == 2)
            {
                var lambda = (LambdaExpression)StripQuotes(node.Arguments[1]);
                Visit(lambda.Body);
                Visit(node.Arguments[0]);
                return node;
            }

            // collection.Contains(x.columns)
            if (node.Method.Name == "Contains" && node.Object == null && node.Arguments.Count == 2 )
            {
                if (TryEvaluateConstans(node.Arguments[0]) is IEnumerable values && node.Arguments[1] is MemberExpression memberExpr)
                {
                    string column = memberExpr.Member.Name.ToLower();
                    var inList = new List<string>();
                    foreach (var val in values)
                    {
                        inList.Add(val is string s ? $"'{s}'" : val.ToString());
                    }
                    _conditions.Add($"{column} IN ({string.Join(", ", inList)})");

                    if (IsColumnFilterable(column) == false && _allowFiltering == false)
                    {
                        _allowFiltering = true;
                        Trace.TraceWarning($" Member name {_tableName}.{column} is used in condition, but it is not indexed field. Can be very slow!");
                    }

                    return node;
                }
            }

            if (node.Method.Name == "Take" && node.Arguments.Count == 2)
            {
                var count = (int)TryEvaluateConstans(node.Arguments[1]);
                _limit = count;
                Visit(node.Arguments[0]);
                return node;
            }

            if ((node.Method.Name == "OrderBy" || node.Method.Name == "OrderByDescending" || node.Method.Name == "ThenBy" || node.Method.Name == "ThenByDescending" ) && node.Arguments.Count == 2)
            {
                var lambda = (LambdaExpression)StripQuotes(node.Arguments[1]);
                if (lambda.Body is MemberExpression memberExpr)
                {
                    string direction = (node.Method.Name.EndsWith("Descending")) ? "DESC" : "ASC";
                    string columnName = memberExpr.Member.Name.ToLower();

                    _orderBy.Add($"{columnName} {direction}");
                }
                else
                {
                    throw new NotSupportedException("Only member expressions are supported in OrderBy/ThenBy clauses.");
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
                    var columnName = memberExpr.Member.Name.ToLower();
                    var targetProp = binding.Member.Name.ToLower();

                    _selectedFields.Add(columnName);
                    _projectionMap[targetProp] = columnName;
                }
            }
            return node;
        }

        internal NewExpression _projectionAnonymousCtor = null;

        protected override Expression VisitNew(NewExpression node)
        {
            _projectionAnonymousCtor = node;

            for (int i = 0; i < node.Members.Count; i++)
            {
                if (node.Arguments[i] is MemberExpression memberExpr)
                {
                    var columnName = memberExpr.Member.Name.ToLower();
                    var targetProp = node.Members[i].Name.ToLower();

                    _selectedFields.Add(columnName);
                    _projectionMap[targetProp] = columnName;
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
