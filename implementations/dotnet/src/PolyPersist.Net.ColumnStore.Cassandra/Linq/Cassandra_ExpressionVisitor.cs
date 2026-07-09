using Cassandra;
using System.Collections;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq.Expressions;

namespace PolyPersist.Net.ColumnStore.Cassandra.Linq
{
    internal class Cassandra_ExpressionVisitor : ExpressionVisitor
    {
        private string _keyspace;
        private string _tableName;
        private TableMetadata _tableMeta;
        private readonly List<string> _conditions = new();
        internal readonly List<object> _parameters = new();   // bound values for '?' placeholders
        private List<string> _orderBy = new();
        private int? _limit = null;
        private bool _allowFiltering = false;

        internal List<string> _selectedFields = new();
        internal Dictionary<string, string> _projectionMap = new();

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

        public (string, Cassandra_Query.ResultTypes) TranslateSelect()
        {
            if (_isCount)
            {
                return (" COUNT(*) ", Cassandra_Query.ResultTypes.Count);
            }
            else if (_selectedFields?.Count == 1 && _projectionAnonymousCtor == null && _projectionMap.Count == 1)
            {
                // Single column projection (primitive)
                return (_selectedFields[0], Cassandra_Query.ResultTypes.ProjectionToSingleColumn);
            }
            else if (_selectedFields?.Count > 0)
            {
                return (string.Join(", ", _selectedFields), _projectionAnonymousCtor != null
                    ? Cassandra_Query.ResultTypes.ProjectionToAnonymous
                    : Cassandra_Query.ResultTypes.ProjectionToClass);
            }
            else
                return ("*", Cassandra_Query.ResultTypes.SelectRow);
        }

        public string TranslateOrderBy() => _orderBy.Count > 0
            ? $" ORDER BY {string.Join(", ", _orderBy)}"
            : "";

        public string TranslateLimit() => _limit.HasValue
            ? $" LIMIT {_limit}"
            : "";

        protected override Expression VisitBinary(BinaryExpression node)
        {
            if (node.NodeType == ExpressionType.OrElse)
                throw new NotSupportedException("Cassandra CQL does not support 'OR' in a WHERE clause; split the query into separate queries instead.");

            if (node.NodeType == ExpressionType.AndAlso)
            {
                Visit(node.Left);
                _conditions.Add("AND");
                Visit(node.Right);
                return node;
            }

            // A "column" is a member access rooted at the lambda parameter (x.Col / x.Col.Length); a
            // captured local (closure field) is ALSO a MemberExpression but is a VALUE, evaluated by
            // TryEvaluateConstans. Normalise so the column is on the left; the query may write it on
            // either side (e.g. "x.Age > 5" or "5 < x.Age", operator flipped).
            bool leftIsColumn = _IsColumnMember(node.Left);
            bool rightIsColumn = _IsColumnMember(node.Right);

            MemberExpression memberExpr;
            Expression valueExpr;
            ExpressionType op;
            if (leftIsColumn && rightIsColumn == false)
            {
                memberExpr = (MemberExpression)node.Left;
                valueExpr = node.Right;
                op = node.NodeType;
            }
            else if (rightIsColumn && leftIsColumn == false)
            {
                memberExpr = (MemberExpression)node.Right;
                valueExpr = node.Left;
                op = _FlipOperator(node.NodeType);
            }
            else if (leftIsColumn && rightIsColumn)
            {
                throw new NotSupportedException("Cassandra CQL cannot compare two columns in a WHERE clause; compare a column to a value.");
            }
            else
            {
                var badMethod = (node.Left as MethodCallExpression) ?? (node.Right as MethodCallExpression);
                if (badMethod != null)
                    throw new NotSupportedException($"Cassandra CQL does not support a method call ('{badMethod.Method.Name}') as a WHERE operand.");
                throw new NotSupportedException($"One side of the comparison must be a column (member); got {node.Left.NodeType} and {node.Right.NodeType}.");
            }

            string left;
            string memberName;
            if (memberExpr.Member.Name == "Length" && memberExpr.Expression is MemberExpression strMember)
            {
                left = $"length({strMember.Member.Name.ToLower()})";
                memberName = strMember.Member.Name.ToLower();

                _allowFiltering = true;
                Trace.TraceWarning($" using 'length' on {_tableName}.{memberName}. Can be very slow!");
            }
            else
            {
                left = memberExpr.Member.Name.ToLower();
                memberName = memberExpr.Member.Name.ToLower();
            }

            if (IsColumnFilterable(memberName) == false && _allowFiltering == false)
            {
                _allowFiltering = true;
                Trace.TraceWarning($" Member name {_tableName}.{memberName} is used in condition, but it is not indexed field. Can be very slow!");
            }

            object? right = TryEvaluateConstans(valueExpr);
            string opSymbol = op switch
            {
                ExpressionType.Equal => "=",
                ExpressionType.NotEqual => throw new NotSupportedException("Cassandra CQL does not support '!=' in a WHERE clause."),
                ExpressionType.GreaterThan => ">",
                ExpressionType.GreaterThanOrEqual => ">=",
                ExpressionType.LessThan => "<",
                ExpressionType.LessThanOrEqual => "<=",
                _ => throw new NotSupportedException($"Operator '{node.NodeType}' not supported")
            };

            // A string value is the CQL-injection vector (a quote in the value would break
            // out of the literal), so bind it as a positional parameter instead of
            // interpolating it. The other types render to safe, unforgeable literals.
            if (right is string s)
            {
                _parameters.Add(s);
                _conditions.Add($"{left} {opSymbol} ?");
            }
            else
            {
                string valStr = right switch
                {
                    DateTime dt => $"'{dt:yyyy-MM-dd HH:mm:ss}'",
                    DateOnly d => $"'{d:yyyy-MM-dd}'",
                    TimeOnly t => ((long)(t.ToTimeSpan().TotalMilliseconds * 1_000_000)).ToString(),
                    Guid g => $"'{g}'",
                    bool b => b.ToString().ToLower(),
                    _ => right?.ToString() ?? string.Empty
                };
                _conditions.Add($"{left} {opSymbol} {valStr}");
            }
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
                else if (lambda.Body is MemberExpression memberExpr)
                {
                    // egyszerű property select: r => r.id
                    var columnName = memberExpr.Member.Name.ToLower();
                    _selectedFields.Add(columnName);
                    _projectionMap[columnName] = columnName;
                }
                else
                {
                    throw new NotSupportedException($"Unsupported Select projection expression: {node.NodeType}.");
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
            if (node.Method.Name == "Contains" && node.Object == null && node.Arguments.Count == 2)
            {
                if (TryEvaluateConstans(node.Arguments[0]) is IEnumerable values && node.Arguments[1] is MemberExpression memberExpr)
                {
                    string column = memberExpr.Member.Name.ToLower();
                    var inList = new List<string>();
                    foreach (var val in values)
                    {
                        inList.Add(val is string s ? $"'{s}'" : val.ToString() ?? string.Empty);
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
                var count = (int)TryEvaluateConstans(node.Arguments[1])!;
                _limit = count;
                Visit(node.Arguments[0]);
                return node;
            }

            if ((node.Method.Name == "OrderBy" || node.Method.Name == "OrderByDescending" || node.Method.Name == "ThenBy" || node.Method.Name == "ThenByDescending") && node.Arguments.Count == 2)
            {
                var lambda = (LambdaExpression)StripQuotes(node.Arguments[1]);
                if (lambda.Body is MemberExpression memberExpr)
                {
                    string direction = (node.Method.Name.EndsWith("Descending")) ? "DESC" : "ASC";
                    string columnName = memberExpr.Member.Name.ToLower();

                    _orderBy.Insert(0, $"{columnName} {direction}");
                }
                else
                {
                    throw new NotSupportedException("Only member expressions are supported in OrderBy/ThenBy clauses.");
                }
                Visit(node.Arguments[0]);
                return node;
            }

            // .Any() with no predicate: we only need to know whether a row exists (LIMIT 1).
            if (node.Method.Name == "Any" && node.Arguments.Count == 1)
            {
                _limit = 1;
                Visit(node.Arguments[0]);
                return node;
            }

            // .First()/.FirstOrDefault()/.Single()/.SingleOrDefault() with no predicate:
            // fetch a single row (LIMIT 1). Use `.Where(...)` for filtering.
            if ((node.Method.Name == "First" || node.Method.Name == "FirstOrDefault"
                 || node.Method.Name == "Single" || node.Method.Name == "SingleOrDefault")
                && node.Arguments.Count == 1)
            {
                _limit = 1;
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

        internal NewExpression? _projectionAnonymousCtor = null;

        protected override Expression VisitNew(NewExpression node)
        {
            _projectionAnonymousCtor = node;

            for (int i = 0; i < node.Members!.Count; i++)
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

        // True when the expression is a column reference: a member access rooted at the lambda
        // parameter (x.Col, x.Col.Length). A captured local renders as a member rooted at a constant
        // (the closure), so it is NOT a column - it is a value evaluated by TryEvaluateConstans.
        private static bool _IsColumnMember(Expression e)
        {
            while (e is MemberExpression m)
            {
                if (m.Expression is ParameterExpression)
                    return true;
                e = m.Expression!;
            }
            return false;
        }

        // Flips a comparison operator when the operands are swapped (column moved to the left).
        private static ExpressionType _FlipOperator(ExpressionType op) => op switch
        {
            ExpressionType.LessThan => ExpressionType.GreaterThan,
            ExpressionType.LessThanOrEqual => ExpressionType.GreaterThanOrEqual,
            ExpressionType.GreaterThan => ExpressionType.LessThan,
            ExpressionType.GreaterThanOrEqual => ExpressionType.LessThanOrEqual,
            _ => op, // Equal / NotEqual are symmetric
        };

        private static object? TryEvaluateConstans(Expression expr)
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
