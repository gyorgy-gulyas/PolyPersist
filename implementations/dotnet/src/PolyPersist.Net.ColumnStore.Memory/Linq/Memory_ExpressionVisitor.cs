using System.Collections;
using System.Linq.Expressions;

namespace PolyPersist.Net.ColumnStore.Memory.Linq
{
    internal class Memory_ExpressionVisitor : ExpressionVisitor
    {
        protected override Expression VisitBinary(BinaryExpression node)
        {
            if (node.NodeType == ExpressionType.AndAlso || node.NodeType == ExpressionType.OrElse)
            {
                Visit(node.Left);
                Visit(node.Right);
                return node;
            }

            if (node.Left is MemberExpression leftMember)
            {
                // ok
            }
            else if (node.Left is MethodCallExpression methodCall)
            {
                throw new NotSupportedException($"Unsupported method call on left side: {methodCall.Method.Name}");
            }
            else
            {
                throw new NotSupportedException($"Left side of binary expression must be a member, not a {node.Left.NodeType}");
            }

            switch(node.Right)
            {
                case ConstantExpression c:
                case MemberExpression m:
                    // ok
                    break;
                default:
                    throw new NotSupportedException("Expression type not supported");
            };

            switch (node.NodeType)
            {
                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                    // ok
                    break;
                default:
                    throw new NotSupportedException($"Operator '{node.NodeType}' not supported");
            }
            ;

            return base.VisitBinary( node );
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {

            if (node.Method.Name == "Count" && node.Arguments.Count == 1)
            {
                // ok
                return base.VisitMethodCall(node);
            }

            if (node.Method.Name == "Distinct" && node.Arguments.Count == 1)
            {
                // ok
                return base.VisitMethodCall(node);
            }

            if (node.Method.Name == "Select" && node.Arguments.Count == 2)
            {
                var lambda = (LambdaExpression)StripQuotes(node.Arguments[1]);

                if (lambda.Body is MemberInitExpression init)
                {
                    // ok
                    return base.VisitMethodCall(node);
                }
                else if (lambda.Body is NewExpression newExpr)
                {
                    // ok
                    return base.VisitMethodCall(node);
                }
                else if (lambda.Body is MemberExpression memberExpr)
                {
                    // ok
                    return base.VisitMethodCall(node);
                }
                else
                {
                    throw new NotSupportedException($"Unsupported Select projection expression: {node.NodeType}.");
                }
            }


            if (node.Method.Name == "Where" && node.Arguments.Count == 2)
            {
                // ok
                return base.VisitMethodCall(node);
            }

            // collection.Contains(x.columns)
            if (node.Method.Name == "Contains" && node.Object == null && node.Arguments.Count == 2)
            {
                if (TryEvaluateConstans(node.Arguments[0]) is IEnumerable values && node.Arguments[1] is MemberExpression memberExpr)
                {
                    // ok
                    return base.VisitMethodCall(node);
                }
            }

            if (node.Method.Name == "Take" && node.Arguments.Count == 2)
            {
                // ok
                return base.VisitMethodCall(node);
            }

            if ((node.Method.Name == "OrderBy" || node.Method.Name == "OrderByDescending" || node.Method.Name == "ThenBy" || node.Method.Name == "ThenByDescending") && node.Arguments.Count == 2)
            {
                var lambda = (LambdaExpression)StripQuotes(node.Arguments[1]);
                if (lambda.Body is MemberExpression memberExpr)
                {
                    // ok
                    return base.VisitMethodCall(node);
                }
                else
                {
                    throw new NotSupportedException("Only member expressions are supported in OrderBy/ThenBy clauses.");
                }
            }

            throw new NotSupportedException($"The method '{node.Method.Name}' is not supported by Cassandra LINQ provider.");
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

        private static Expression StripQuotes(Expression e)
        {
            while (e.NodeType == ExpressionType.Quote)
            {
                e = ((UnaryExpression)e).Operand;
            }
            return e;
        }

    }
}
