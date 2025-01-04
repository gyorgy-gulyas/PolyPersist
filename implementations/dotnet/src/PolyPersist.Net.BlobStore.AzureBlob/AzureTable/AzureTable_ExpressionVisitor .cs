using System.Linq.Expressions;
using System.Text;

namespace PolyPersist.Net.BlobStore.AzureBlob.AzureTable
{
    internal class AzureTable_ExpressionVisitor : ExpressionVisitor
    {
        private StringBuilder _filterBuilder;

        public static string GetODataFilter(Expression expression)
        {
            var visitor = new AzureTable_ExpressionVisitor();
            visitor._filterBuilder = new StringBuilder();
            visitor.Visit(expression);
            return visitor._filterBuilder.ToString();
        }

        protected override Expression VisitBinary(BinaryExpression node)
        {
            _filterBuilder.Append("(");
            Visit(node.Left);

            // Művelet átalakítása OData kompatibilis formátumra
            _filterBuilder.Append(node.NodeType switch
            {
                ExpressionType.Equal => " eq ",
                ExpressionType.NotEqual => " ne ",
                ExpressionType.LessThan => " lt ",
                ExpressionType.LessThanOrEqual => " le ",
                ExpressionType.GreaterThan => " gt ",
                ExpressionType.GreaterThanOrEqual => " ge ",
                _ => throw new NotSupportedException($"Operation {node.NodeType} is not supported.")
            });

            Visit(node.Right);
            _filterBuilder.Append(")");
            return node;
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            if (node.Type == typeof(string))
            {
                _filterBuilder.Append($"'{node.Value}'");
            }
            else
            {
                _filterBuilder.Append(node.Value);
            }
            return node;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            _filterBuilder.Append(node.Member.Name);
            return node;
        }
    }
}
