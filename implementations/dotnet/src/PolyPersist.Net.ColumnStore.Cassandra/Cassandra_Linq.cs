using Cassandra;
using PolyPersist.Net.Common;
using System.Collections;
using System.Linq.Expressions;

namespace PolyPersist.Net.ColumnStore.Cassandra
{
    internal class Cassandra_Queryable<TRow> : IQueryable<TRow>
        where TRow : IRow, new()
    {
        private readonly Expression _expression;
        private readonly Cassandra_QueryProvider<TRow> _provider;

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

        public IEnumerator<TRow> GetEnumerator()
        {
            return _provider
                .Execute<IEnumerable<TRow>>(_expression)
                .GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
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

        public object Execute(Expression expression)
        {
            var visitor = new Cassandra_ExpressionVisitor();
            string whereClause = visitor.TranslateWhere(expression);
            string selectClause = visitor.TranslateSelect(expression) ?? "*";
            string orderByClause = visitor.TranslateOrderBy();
            string limitClause = visitor.TranslateLimit();

            var cql = $"SELECT {selectClause} FROM {_table._tableName}{whereClause}{orderByClause}{limitClause};";

            var rs = _table._session.Execute(new SimpleStatement(cql));
            var accessors = MetadataHelper
                .GetAccessors<TRow>()
                .ToDictionary(kvp => kvp.Key.ToLower(), kvp => kvp.Value);

            // csak a szükséges mezőket használjuk
            var usedAccessors = (visitor.SelectedFieldSet != null && visitor.SelectedFieldSet.Count > 0)
                ? accessors.Where(kvp => visitor.SelectedFieldSet.Contains(kvp.Key)).ToList()
                : accessors.ToList();

            //  build index map, for reaching values in row based on index instad of name
            var columns = rs.Columns;
            var fieldIndexMap = new Dictionary<string, int>();
            for (int i = 0; i < columns.Length; i++)
                fieldIndexMap[columns[i].Name.ToLower()] = i;

            var mappedAccessors = usedAccessors
                .Select(kvp => (
                    FieldIndex: fieldIndexMap[kvp.Key],
                    MemberAccessor: kvp.Value,
                    CassandraValueGetter: Cassandra_ValueHelper.BuildTypedGetter(kvp.Value.Type)))
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

        public TResult Execute<TResult>(Expression expression)
        {
            return (TResult)Execute(expression);
        }
    }

    internal class Cassandra_ExpressionVisitor : ExpressionVisitor
    {
        private readonly List<string> _conditions = new();
        private string _orderBy = null;
        private int? _limit = null;

        private List<string> _selectedFields;
        private HashSet<string> _selectedFieldSet;
        public HashSet<string> SelectedFieldSet =>
            _selectedFieldSet ??= _selectedFields != null
                ? new HashSet<string>(_selectedFields)
                : null;

        public string TranslateWhere(Expression expression)
        {
            Visit(expression);
            return _conditions.Count > 0 ? " WHERE " + string.Join(" AND ", _conditions) : "";
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
            var left = ((MemberExpression)node.Left).Member.Name.ToLower();
            var right = TryEvaluateConstans(node.Right);
            var op = node.NodeType switch
            {
                ExpressionType.GreaterThan => ">",
                ExpressionType.LessThan => "<",
                ExpressionType.Equal => "=",
                ExpressionType.GreaterThanOrEqual => ">=",
                ExpressionType.LessThanOrEqual => "<=",
                _ => throw new NotSupportedException("Operator not supported")
            };

            var valStr = right switch
            {
                string s => $"'{s}'",
                DateTime dt => $"'{dt.ToUniversalTime():yyyy-MM-dd HH:mm:ss}'",
                DateOnly d => $"'{d:yyyy-MM-dd}'",
                TimeOnly t => ((long)(t.ToTimeSpan().TotalMilliseconds * 1_000_000)).ToString(),
                Guid g => $"'{g}'",
                _ => right.ToString()
            };

            _conditions.Add($"{left} {op} {valStr}");
            return node;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
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
                if (binding is MemberAssignment assignment &&
                    assignment.Expression is MemberExpression memberExpr)
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


    internal static class Cassandra_ValueHelper
    {
        internal delegate object GetterDelegate(Row row, int index);

        internal static GetterDelegate BuildTypedGetter(Type type)
        {
            var method = typeof(Row).GetMethod("GetValue", new[] { typeof(int) }).MakeGenericMethod(type);
            var rowParam = Expression.Parameter(typeof(Row), "row");
            var indexParam = Expression.Parameter(typeof(int), "index");

            var call = Expression.Call(rowParam, method, indexParam);
            var lambda = Expression.Lambda<GetterDelegate>(Expression.Convert(call, typeof(object)), rowParam, indexParam);
            return lambda.Compile();
        }
    }

}
